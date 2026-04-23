/*--------------------------------------------------------------------------
 *
 * injection_points.c
 *		Code for testing injection points.
 *
 * Injection points are able to trigger user-defined callbacks in pre-defined
 * code paths.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *		src/test/modules/injection_points/injection_points.c
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/csn_mvcc_vars.h"
#include "access/transam.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/pg_list.h"
#include "nodes/value.h"
#include "storage/condition_variable.h"
#include "storage/dsm_registry.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/shmem.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/injection_point.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/tuplestore.h"
#include "utils/wait_event.h"
#include "utils/xid8.h"

PG_MODULE_MAGIC;

/* Maximum number of waits usable in injection points at once */
#define INJ_MAX_WAIT	8
#define INJ_MAX_COUNT	16
#define INJ_MAX_INT8	16
#define INJ_NAME_MAXLEN	64

/*
 * Conditions related to injection points.  This tracks in shared memory the
 * runtime conditions under which an injection point is allowed to run,
 * stored as private_data when an injection point is attached, and passed as
 * argument to the callback.
 *
 * If more types of runtime conditions need to be tracked, this structure
 * should be expanded.
 */
typedef enum InjectionPointConditionType
{
	INJ_CONDITION_ALWAYS = 0,	/* always run */
	INJ_CONDITION_PID,			/* PID restriction */
} InjectionPointConditionType;

typedef struct InjectionPointCondition
{
	/* Type of the condition */
	InjectionPointConditionType type;

	/* ID of the process where the injection point is allowed to run */
	int			pid;
} InjectionPointCondition;

/*
 * List of injection points stored in TopMemoryContext attached
 * locally to this process.
 */
static List *inj_list_local = NIL;
static int64 inj_saved_int8 = 0;
static bool inj_saved_int8_valid = false;
static FullTransactionId inj_saved_fxid;
static bool inj_saved_fxid_valid = false;

/*
 * Shared state information for injection points.
 *
 * This state data can be initialized in two ways: dynamically with a DSM
 * or when loading the module.
 */
typedef struct InjectionPointSharedState
{
	/* Protects access to other fields */
	slock_t		lock;

	/* Counters advancing when injection_points_wakeup() is called */
	uint32		wait_counts[INJ_MAX_WAIT];

	/* Names of injection points attached to wait counters */
	char		name[INJ_MAX_WAIT][INJ_NAME_MAXLEN];

	/* Condition variable used for waits and wakeups */
	ConditionVariable wait_point;

	/* Hit counters advanced by the count action */
	uint32		count_hits[INJ_MAX_COUNT];

	/* Names of injection points tracked by count action */
	char		count_name[INJ_MAX_COUNT][INJ_NAME_MAXLEN];

	/* Shared int8 slots for cross-backend test coordination */
	int64		int8_values[INJ_MAX_INT8];

	/* Names of shared int8 slots */
	char		int8_name[INJ_MAX_INT8][INJ_NAME_MAXLEN];
} InjectionPointSharedState;

/* Pointer to shared-memory state. */
static InjectionPointSharedState *inj_state = NULL;

extern PGDLLEXPORT void injection_error(const char *name,
										const void *private_data,
										void *arg);
extern PGDLLEXPORT void injection_notice(const char *name,
										 const void *private_data,
										 void *arg);
extern PGDLLEXPORT void injection_count(const char *name,
										const void *private_data,
										void *arg);
extern PGDLLEXPORT void injection_wait(const char *name,
									   const void *private_data,
									   void *arg);

/* track if injection points attached in this process are linked to it */
static bool injection_point_local = false;

static void injection_shmem_request(void *arg);
static void injection_shmem_init(void *arg);
static void injection_init_shmem(void);

static const ShmemCallbacks injection_shmem_callbacks = {
	.request_fn = injection_shmem_request,
	.init_fn = injection_shmem_init,
};

/*
 * Routine for shared memory area initialization, used as a callback
 * when initializing dynamically with a DSM or when loading the module.
 */
static void
injection_point_init_state(void *ptr, void *arg)
{
	InjectionPointSharedState *state = (InjectionPointSharedState *) ptr;

	SpinLockInit(&state->lock);
	memset(state->wait_counts, 0, sizeof(state->wait_counts));
	memset(state->name, 0, sizeof(state->name));
	ConditionVariableInit(&state->wait_point);
	memset(state->count_hits, 0, sizeof(state->count_hits));
	memset(state->count_name, 0, sizeof(state->count_name));
	memset(state->int8_values, 0, sizeof(state->int8_values));
	memset(state->int8_name, 0, sizeof(state->int8_name));
}

static int
injection_count_lookup_locked(const char *name, bool create)
{
	int			free_index = -1;

	for (int i = 0; i < INJ_MAX_COUNT; i++)
	{
		if (inj_state->count_name[i][0] == '\0')
		{
			if (free_index < 0)
				free_index = i;
			continue;
		}

		if (strcmp(name, inj_state->count_name[i]) == 0)
			return i;
	}

	if (!create || free_index < 0)
		return -1;

	strlcpy(inj_state->count_name[free_index], name, INJ_NAME_MAXLEN);
	inj_state->count_hits[free_index] = 0;

	return free_index;
}

static uint32
injection_count_get(const char *name)
{
	int			index;
	uint32		result = 0;

	if (inj_state == NULL)
		injection_init_shmem();

	SpinLockAcquire(&inj_state->lock);
	index = injection_count_lookup_locked(name, false);
	if (index >= 0)
		result = inj_state->count_hits[index];
	SpinLockRelease(&inj_state->lock);

	return result;
}

static void
injection_count_reset(const char *name)
{
	int			index;

	if (inj_state == NULL)
		injection_init_shmem();

	SpinLockAcquire(&inj_state->lock);
	index = injection_count_lookup_locked(name, true);
	if (index < 0)
	{
		SpinLockRelease(&inj_state->lock);
		elog(ERROR, "could not find free slot for count of injection point %s",
			 name);
	}
	inj_state->count_hits[index] = 0;
	SpinLockRelease(&inj_state->lock);
}

static int
injection_int8_lookup_locked(const char *name, bool create)
{
	int			free_index = -1;

	for (int i = 0; i < INJ_MAX_INT8; i++)
	{
		if (inj_state->int8_name[i][0] == '\0')
		{
			if (free_index < 0)
				free_index = i;
			continue;
		}

		if (strcmp(name, inj_state->int8_name[i]) == 0)
			return i;
	}

	if (!create || free_index < 0)
		return -1;

	strlcpy(inj_state->int8_name[free_index], name, INJ_NAME_MAXLEN);
	inj_state->int8_values[free_index] = 0;

	return free_index;
}

static void
injection_int8_set(const char *name, int64 value)
{
	int			index;

	if (inj_state == NULL)
		injection_init_shmem();

	SpinLockAcquire(&inj_state->lock);
	index = injection_int8_lookup_locked(name, true);
	if (index < 0)
	{
		SpinLockRelease(&inj_state->lock);
		elog(ERROR, "could not find free slot for int8 value of injection point %s",
			 name);
	}
	inj_state->int8_values[index] = value;
	SpinLockRelease(&inj_state->lock);
}

static bool
injection_int8_get(const char *name, int64 *value)
{
	int			index;
	bool		found = false;

	if (inj_state == NULL)
		injection_init_shmem();

	SpinLockAcquire(&inj_state->lock);
	index = injection_int8_lookup_locked(name, false);
	if (index >= 0)
	{
		*value = inj_state->int8_values[index];
		found = true;
	}
	SpinLockRelease(&inj_state->lock);

	return found;
}

static void
injection_shmem_request(void *arg)
{
	ShmemRequestStruct(.name = "injection_points",
					   .size = sizeof(InjectionPointSharedState),
					   .ptr = (void **) &inj_state,
		);
}

static void
injection_shmem_init(void *arg)
{
	/*
	 * First time through, so initialize.  This is shared with the dynamic
	 * initialization using a DSM.
	 */
	injection_point_init_state(inj_state, NULL);
}

/*
 * Initialize shared memory area for this module through DSM.
 */
static void
injection_init_shmem(void)
{
	bool		found;

	if (inj_state != NULL)
		return;

	inj_state = GetNamedDSMSegment("injection_points",
								   sizeof(InjectionPointSharedState),
								   injection_point_init_state,
								   &found, NULL);
}

/*
 * Check runtime conditions associated to an injection point.
 *
 * Returns true if the named injection point is allowed to run, and false
 * otherwise.
 */
static bool
injection_point_allowed(const InjectionPointCondition *condition)
{
	bool		result = true;

	switch (condition->type)
	{
		case INJ_CONDITION_PID:
			if (MyProcPid != condition->pid)
				result = false;
			break;
		case INJ_CONDITION_ALWAYS:
			break;
	}

	return result;
}

/*
 * before_shmem_exit callback to remove injection points linked to a
 * specific process.
 */
static void
injection_points_cleanup(int code, Datum arg)
{
	ListCell   *lc;

	/* Leave if nothing is tracked locally */
	if (!injection_point_local)
		return;

	/* Detach all the local points */
	foreach(lc, inj_list_local)
	{
		char	   *name = strVal(lfirst(lc));

		(void) InjectionPointDetach(name);
	}
}

/* Set of callbacks available to be attached to an injection point. */
void
injection_error(const char *name, const void *private_data, void *arg)
{
	const InjectionPointCondition *condition = private_data;
	char	   *argstr = arg;

	if (!injection_point_allowed(condition))
		return;

	if (argstr)
		elog(ERROR, "error triggered for injection point %s (%s)",
			 name, argstr);
	else
		elog(ERROR, "error triggered for injection point %s", name);
}

void
injection_notice(const char *name, const void *private_data, void *arg)
{
	const InjectionPointCondition *condition = private_data;
	char	   *argstr = arg;

	if (!injection_point_allowed(condition))
		return;

	if (argstr)
		elog(NOTICE, "notice triggered for injection point %s (%s)",
			 name, argstr);
	else
		elog(NOTICE, "notice triggered for injection point %s", name);
}

void
injection_count(const char *name, const void *private_data, void *arg)
{
	int			index;
	const InjectionPointCondition *condition = private_data;

	if (inj_state == NULL)
		injection_init_shmem();

	if (!injection_point_allowed(condition))
		return;

	SpinLockAcquire(&inj_state->lock);
	index = injection_count_lookup_locked(name, true);
	if (index < 0)
	{
		SpinLockRelease(&inj_state->lock);
		elog(ERROR, "could not find free slot for count of injection point %s",
			 name);
	}
	inj_state->count_hits[index]++;
	SpinLockRelease(&inj_state->lock);
}

/* Wait on a condition variable, awaken by injection_points_wakeup() */
void
injection_wait(const char *name, const void *private_data, void *arg)
{
	uint32		old_wait_counts = 0;
	int			index = -1;
	uint32		injection_wait_event = 0;
	const InjectionPointCondition *condition = private_data;

	if (inj_state == NULL)
		injection_init_shmem();

	if (!injection_point_allowed(condition))
		return;

	/*
	 * Use the injection point name for this custom wait event.  Note that
	 * this custom wait event name is not released, but we don't care much for
	 * testing as this should be short-lived.
	 */
	injection_wait_event = WaitEventInjectionPointNew(name);

	/*
	 * Find a free slot to wait for, and register this injection point's name.
	 */
	SpinLockAcquire(&inj_state->lock);
	for (int i = 0; i < INJ_MAX_WAIT; i++)
	{
		if (inj_state->name[i][0] == '\0')
		{
			index = i;
			strlcpy(inj_state->name[i], name, INJ_NAME_MAXLEN);
			old_wait_counts = inj_state->wait_counts[i];
			break;
		}
	}
	SpinLockRelease(&inj_state->lock);

	if (index < 0)
		elog(ERROR, "could not find free slot for wait of injection point %s ",
			 name);

	/* And sleep.. */
	ConditionVariablePrepareToSleep(&inj_state->wait_point);
	for (;;)
	{
		uint32		new_wait_counts;

		SpinLockAcquire(&inj_state->lock);
		new_wait_counts = inj_state->wait_counts[index];
		SpinLockRelease(&inj_state->lock);

		if (old_wait_counts != new_wait_counts)
			break;
		ConditionVariableSleep(&inj_state->wait_point, injection_wait_event);
	}
	ConditionVariableCancelSleep();

	/* Remove this injection point from the waiters. */
	SpinLockAcquire(&inj_state->lock);
	inj_state->name[index][0] = '\0';
	SpinLockRelease(&inj_state->lock);
}

/*
 * SQL function for creating an injection point.
 */
PG_FUNCTION_INFO_V1(injection_points_attach);
Datum
injection_points_attach(PG_FUNCTION_ARGS)
{
	char	   *name = text_to_cstring(PG_GETARG_TEXT_PP(0));
	char	   *action = text_to_cstring(PG_GETARG_TEXT_PP(1));
	char	   *function;
	InjectionPointCondition condition = {0};

	if (strcmp(action, "error") == 0)
		function = "injection_error";
	else if (strcmp(action, "notice") == 0)
		function = "injection_notice";
	else if (strcmp(action, "count") == 0)
	{
		function = "injection_count";
		injection_count_reset(name);
	}
	else if (strcmp(action, "wait") == 0)
		function = "injection_wait";
	else
		elog(ERROR, "incorrect action \"%s\" for injection point creation", action);

	if (injection_point_local)
	{
		condition.type = INJ_CONDITION_PID;
		condition.pid = MyProcPid;
	}

	InjectionPointAttach(name, "injection_points", function, &condition,
						 sizeof(InjectionPointCondition));

	if (injection_point_local)
	{
		MemoryContext oldctx;

		/* Local injection point, so track it for automated cleanup */
		oldctx = MemoryContextSwitchTo(TopMemoryContext);
		inj_list_local = lappend(inj_list_local, makeString(pstrdup(name)));
		MemoryContextSwitchTo(oldctx);
	}

	PG_RETURN_VOID();
}

/*
 * SQL function for creating an injection point with library name, function
 * name and private data.
 */
PG_FUNCTION_INFO_V1(injection_points_attach_func);
Datum
injection_points_attach_func(PG_FUNCTION_ARGS)
{
	char	   *name;
	char	   *lib_name;
	char	   *function;
	bytea	   *private_data = NULL;
	int			private_data_size = 0;

	if (PG_ARGISNULL(0))
		elog(ERROR, "injection point name cannot be NULL");
	if (PG_ARGISNULL(1))
		elog(ERROR, "injection point library cannot be NULL");
	if (PG_ARGISNULL(2))
		elog(ERROR, "injection point function cannot be NULL");

	name = text_to_cstring(PG_GETARG_TEXT_PP(0));
	lib_name = text_to_cstring(PG_GETARG_TEXT_PP(1));
	function = text_to_cstring(PG_GETARG_TEXT_PP(2));

	if (!PG_ARGISNULL(3))
	{
		private_data = PG_GETARG_BYTEA_PP(3);
		private_data_size = VARSIZE_ANY_EXHDR(private_data);
	}

	if (private_data != NULL)
		InjectionPointAttach(name, lib_name, function, VARDATA_ANY(private_data),
							 private_data_size);
	else
		InjectionPointAttach(name, lib_name, function, NULL,
							 0);
	PG_RETURN_VOID();
}

/*
 * SQL function for loading an injection point.
 */
PG_FUNCTION_INFO_V1(injection_points_load);
Datum
injection_points_load(PG_FUNCTION_ARGS)
{
	char	   *name = text_to_cstring(PG_GETARG_TEXT_PP(0));

	if (inj_state == NULL)
		injection_init_shmem();

	INJECTION_POINT_LOAD(name);

	PG_RETURN_VOID();
}

/*
 * SQL function for triggering an injection point.
 */
PG_FUNCTION_INFO_V1(injection_points_run);
Datum
injection_points_run(PG_FUNCTION_ARGS)
{
	char	   *name;
	char	   *arg = NULL;

	if (PG_ARGISNULL(0))
		PG_RETURN_VOID();
	name = text_to_cstring(PG_GETARG_TEXT_PP(0));

	if (!PG_ARGISNULL(1))
		arg = text_to_cstring(PG_GETARG_TEXT_PP(1));

	INJECTION_POINT(name, arg);

	PG_RETURN_VOID();
}

/*
 * SQL function for triggering an injection point from cache.
 */
PG_FUNCTION_INFO_V1(injection_points_cached);
Datum
injection_points_cached(PG_FUNCTION_ARGS)
{
	char	   *name;
	char	   *arg = NULL;

	if (PG_ARGISNULL(0))
		PG_RETURN_VOID();
	name = text_to_cstring(PG_GETARG_TEXT_PP(0));

	if (!PG_ARGISNULL(1))
		arg = text_to_cstring(PG_GETARG_TEXT_PP(1));

	INJECTION_POINT_CACHED(name, arg);

	PG_RETURN_VOID();
}

/*
 * SQL function for waking up an injection point waiting in injection_wait().
 */
PG_FUNCTION_INFO_V1(injection_points_wakeup);
Datum
injection_points_wakeup(PG_FUNCTION_ARGS)
{
	char	   *name = text_to_cstring(PG_GETARG_TEXT_PP(0));
	int			index = -1;

	if (inj_state == NULL)
		injection_init_shmem();

	/* First bump the wait counter for the injection point to wake up */
	SpinLockAcquire(&inj_state->lock);
	for (int i = 0; i < INJ_MAX_WAIT; i++)
	{
		if (strcmp(name, inj_state->name[i]) == 0)
		{
			index = i;
			break;
		}
	}
	if (index < 0)
	{
		SpinLockRelease(&inj_state->lock);
		elog(ERROR, "could not find injection point %s to wake up", name);
	}
	inj_state->wait_counts[index]++;
	SpinLockRelease(&inj_state->lock);

	/* And broadcast the change to the waiters */
	ConditionVariableBroadcast(&inj_state->wait_point);
	PG_RETURN_VOID();
}

/*
 * injection_points_set_local
 *
 * Track if any injection point created in this process ought to run only
 * in this process.  Such injection points are detached automatically when
 * this process exits.  This is useful to make test suites concurrent-safe.
 */
PG_FUNCTION_INFO_V1(injection_points_set_local);
Datum
injection_points_set_local(PG_FUNCTION_ARGS)
{
	/* Enable flag to add a runtime condition based on this process ID */
	injection_point_local = true;

	if (inj_state == NULL)
		injection_init_shmem();

	/*
	 * Register a before_shmem_exit callback to remove any injection points
	 * linked to this process.
	 */
	before_shmem_exit(injection_points_cleanup, (Datum) 0);

	PG_RETURN_VOID();
}

/*
 * SQL function for reading a count-action hit counter.
 */
PG_FUNCTION_INFO_V1(injection_points_get_count);
Datum
injection_points_get_count(PG_FUNCTION_ARGS)
{
	char	   *name = text_to_cstring(PG_GETARG_TEXT_PP(0));

	PG_RETURN_INT32((int32) injection_count_get(name));
}

/*
 * SQL function for resetting a count-action hit counter.
 */
PG_FUNCTION_INFO_V1(injection_points_reset_count);
Datum
injection_points_reset_count(PG_FUNCTION_ARGS)
{
	char	   *name = text_to_cstring(PG_GETARG_TEXT_PP(0));

	injection_count_reset(name);
	PG_RETURN_VOID();
}

/*
 * SQL functions for storing and reading shared int8 values across backends.
 */
PG_FUNCTION_INFO_V1(injection_points_set_global_int8);
Datum
injection_points_set_global_int8(PG_FUNCTION_ARGS)
{
	char	   *name = text_to_cstring(PG_GETARG_TEXT_PP(0));
	int64		value = PG_GETARG_INT64(1);

	injection_int8_set(name, value);
	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(injection_points_get_global_int8);
Datum
injection_points_get_global_int8(PG_FUNCTION_ARGS)
{
	char	   *name = text_to_cstring(PG_GETARG_TEXT_PP(0));
	int64		value;

	if (!injection_int8_get(name, &value))
		PG_RETURN_NULL();

	PG_RETURN_INT64(value);
}

/*
 * SQL function for exposing TransactionIdIsInProgress() to SQL tests.
 */
PG_FUNCTION_INFO_V1(injection_points_xid_in_progress);
Datum
injection_points_xid_in_progress(PG_FUNCTION_ARGS)
{
	FullTransactionId fxid = PG_GETARG_FULLTRANSACTIONID(0);
	TransactionId xid = XidFromFullTransactionId(fxid);

	if (!TransactionIdIsValid(xid))
		PG_RETURN_BOOL(false);

	PG_RETURN_BOOL(TransactionIdIsInProgress(xid));
}

/*
 * SQL function for reading a backend's current top-level xid.
 */
PG_FUNCTION_INFO_V1(injection_points_backend_xid);
Datum
injection_points_backend_xid(PG_FUNCTION_ARGS)
{
	int			pid = PG_GETARG_INT32(0);
	PGPROC	   *proc;
	TransactionId xid = InvalidTransactionId;

	LWLockAcquire(ProcArrayLock, LW_SHARED);
	proc = BackendPidGetProcWithLock(pid);
	if (proc != NULL)
		xid = ProcGlobal->xids[proc->pgxactoff];
	LWLockRelease(ProcArrayLock);

	if (!TransactionIdIsValid(xid))
		PG_RETURN_NULL();

	PG_RETURN_FULLTRANSACTIONID(FullTransactionIdFromU64((uint64) xid));
}

/*
 * SQL function for reading the current slot epoch of the backend with the
 * given PID.
 */
PG_FUNCTION_INFO_V1(injection_points_backend_slot_epoch);
Datum
injection_points_backend_slot_epoch(PG_FUNCTION_ARGS)
{
	int			pid = PG_GETARG_INT32(0);
	PGPROC	   *proc;
	uint64		epoch = 0;

	LWLockAcquire(ProcArrayLock, LW_SHARED);
	proc = BackendPidGetProcWithLock(pid);
	if (proc != NULL)
		epoch = ProcArrayReadSlotEpoch(GetNumberFromPGProc(proc));
	LWLockRelease(ProcArrayLock);

	if (epoch == 0)
		PG_RETURN_NULL();

	PG_RETURN_INT64((int64) epoch);
}

/*
 * SQL function for exposing GetOldestActiveTransactionId() to SQL tests.
 */
PG_FUNCTION_INFO_V1(injection_points_oldest_active_xid);
Datum
injection_points_oldest_active_xid(PG_FUNCTION_ARGS)
{
	bool		in_commit_only = PG_GETARG_BOOL(0);
	bool		all_dbs = PG_GETARG_BOOL(1);
	TransactionId xid;

	xid = GetOldestActiveTransactionId(in_commit_only, all_dbs);

	if (!TransactionIdIsValid(xid))
		PG_RETURN_NULL();

	PG_RETURN_FULLTRANSACTIONID(FullTransactionIdFromU64((uint64) xid));
}

/*
 * SQL function for exposing ReadCSNOldestActiveXid() to SQL tests.
 */
PG_FUNCTION_INFO_V1(injection_points_csn_oldest_active_xid);
Datum
injection_points_csn_oldest_active_xid(PG_FUNCTION_ARGS)
{
	TransactionId xid;

	xid = ReadCSNOldestActiveXid();

	if (!TransactionIdIsValid(xid))
		PG_RETURN_NULL();

	PG_RETURN_FULLTRANSACTIONID(FullTransactionIdFromU64((uint64) xid));
}

/*
 * SQL function for exposing GetOldestTransactionIdConsideredRunning() to SQL
 * tests.
 */
PG_FUNCTION_INFO_V1(injection_points_oldest_considered_running_xid);
Datum
injection_points_oldest_considered_running_xid(PG_FUNCTION_ARGS)
{
	TransactionId xid;

	xid = GetOldestTransactionIdConsideredRunning();

	if (!TransactionIdIsValid(xid))
		PG_RETURN_NULL();

	PG_RETURN_FULLTRANSACTIONID(FullTransactionIdFromU64((uint64) xid));
}

/*
 * SQL function for exposing GetOldestNonRemovableTransactionId(NULL) to SQL
 * tests.
 */
PG_FUNCTION_INFO_V1(injection_points_oldest_nonremovable_xid);
Datum
injection_points_oldest_nonremovable_xid(PG_FUNCTION_ARGS)
{
	TransactionId xid;

	xid = GetOldestNonRemovableTransactionId(NULL);

	if (!TransactionIdIsValid(xid))
		PG_RETURN_NULL();

	PG_RETURN_FULLTRANSACTIONID(FullTransactionIdFromU64((uint64) xid));
}

/*
 * SQL function for exposing TransamVariables->latestCompletedXid to SQL
 * tests.
 */
PG_FUNCTION_INFO_V1(injection_points_latest_completed_xid);
Datum
injection_points_latest_completed_xid(PG_FUNCTION_ARGS)
{
	FullTransactionId latestCompleted;
	TransactionId xid;

	LWLockAcquire(ProcArrayLock, LW_SHARED);
	latestCompleted = TransamVariables->latestCompletedXid;
	LWLockRelease(ProcArrayLock);

	xid = XidFromFullTransactionId(latestCompleted);
	if (!TransactionIdIsValid(xid))
		PG_RETURN_NULL();

	PG_RETURN_FULLTRANSACTIONID(FullTransactionIdFromU64((uint64) xid));
}

/*
 * SQL function for exposing the passive H1-D latestCompletedXid shadow to SQL
 * tests.
 */
PG_FUNCTION_INFO_V1(injection_points_latest_completed_xid_shadow);
Datum
injection_points_latest_completed_xid_shadow(PG_FUNCTION_ARGS)
{
	FullTransactionId latestCompleted;
	TransactionId xid;

	latestCompleted = ProcArrayReadLatestCompletedXidShadow();
	xid = XidFromFullTransactionId(latestCompleted);
	if (!TransactionIdIsValid(xid))
		PG_RETURN_NULL();

	PG_RETURN_FULLTRANSACTIONID(FullTransactionIdFromU64((uint64) xid));
}

/*
 * SQL function for exposing TransamVariables->xactCompletionCount to SQL
 * tests.
 */
PG_FUNCTION_INFO_V1(injection_points_xact_completion_count);
Datum
injection_points_xact_completion_count(PG_FUNCTION_ARGS)
{
	uint64		completionCount;

	LWLockAcquire(ProcArrayLock, LW_SHARED);
	completionCount = TransamVariables->xactCompletionCount;
	LWLockRelease(ProcArrayLock);

	PG_RETURN_INT64((int64) completionCount);
}

/*
 * SQL function for exposing the passive H1-D xactCompletionCount shadow to
 * SQL tests.
 */
PG_FUNCTION_INFO_V1(injection_points_xact_completion_count_shadow);
Datum
injection_points_xact_completion_count_shadow(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT64((int64) TransamReadXactCompletionCountShadow());
}

/*
 * SQL function for exposing the active query snapshot's
 * snapXactCompletionCount to SQL tests.
 */
PG_FUNCTION_INFO_V1(injection_points_active_snapshot_xact_completion_count);
Datum
injection_points_active_snapshot_xact_completion_count(PG_FUNCTION_ARGS)
{
	Snapshot	snapshot;

	if (!ActiveSnapshotSet())
		PG_RETURN_NULL();

	snapshot = GetActiveSnapshot();
	PG_RETURN_INT64((int64) snapshot->snapXactCompletionCount);
}

/*
 * SQL function for exposing GetTransactionSnapshot()->snapXactCompletionCount
 * to SQL tests.
 */
PG_FUNCTION_INFO_V1(injection_points_transaction_snapshot_xact_completion_count);
Datum
injection_points_transaction_snapshot_xact_completion_count(PG_FUNCTION_ARGS)
{
	Snapshot	snapshot;

	snapshot = GetTransactionSnapshot();
	PG_RETURN_INT64((int64) snapshot->snapXactCompletionCount);
}

/*
 * SQL functions for storing and re-reading a backend-local int8 value across
 * isolation test steps without touching shared database state.
 */
PG_FUNCTION_INFO_V1(injection_points_save_int8);
Datum
injection_points_save_int8(PG_FUNCTION_ARGS)
{
	inj_saved_int8 = PG_GETARG_INT64(0);
	inj_saved_int8_valid = true;

	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(injection_points_get_saved_int8);
Datum
injection_points_get_saved_int8(PG_FUNCTION_ARGS)
{
	if (!inj_saved_int8_valid)
		PG_RETURN_NULL();

	PG_RETURN_INT64(inj_saved_int8);
}

/*
 * SQL functions for storing and re-reading a backend-local xid8 value across
 * isolation test steps without touching shared database state.
 */
PG_FUNCTION_INFO_V1(injection_points_save_xid8);
Datum
injection_points_save_xid8(PG_FUNCTION_ARGS)
{
	inj_saved_fxid = PG_GETARG_FULLTRANSACTIONID(0);
	inj_saved_fxid_valid = true;

	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(injection_points_get_saved_xid8);
Datum
injection_points_get_saved_xid8(PG_FUNCTION_ARGS)
{
	if (!inj_saved_fxid_valid)
		PG_RETURN_NULL();

	PG_RETURN_FULLTRANSACTIONID(inj_saved_fxid);
}

/*
 * SQL function for checking whether a backend appears in
 * GetVirtualXIDsDelayingChkpt().
 */
PG_FUNCTION_INFO_V1(injection_points_backend_delays_checkpoint);
Datum
injection_points_backend_delays_checkpoint(PG_FUNCTION_ARGS)
{
	int			pid = PG_GETARG_INT32(0);
	int			type = PG_GETARG_INT32(1);
	int			nvxids = 0;
	VirtualTransactionId target;
	VirtualTransactionId *vxids;
	PGPROC	   *proc;
	bool		found = false;

	SetInvalidVirtualTransactionId(target);

	LWLockAcquire(ProcArrayLock, LW_SHARED);
	proc = BackendPidGetProcWithLock(pid);
	if (proc != NULL)
		GET_VXID_FROM_PGPROC(target, *proc);
	LWLockRelease(ProcArrayLock);

	if (!VirtualTransactionIdIsValid(target))
		PG_RETURN_BOOL(false);

	vxids = GetVirtualXIDsDelayingChkpt(&nvxids, type);
	for (int i = 0; i < nvxids; i++)
	{
		if (VirtualTransactionIdEquals(target, vxids[i]))
		{
			found = true;
			break;
		}
	}
	pfree(vxids);

	PG_RETURN_BOOL(found);
}

/*
 * SQL function for checking whether a backend's xid is present in
 * GetRunningTransactionData().
 */
PG_FUNCTION_INFO_V1(injection_points_running_xacts_include_backend);
Datum
injection_points_running_xacts_include_backend(PG_FUNCTION_ARGS)
{
	int			pid = PG_GETARG_INT32(0);
	bool		current_db_only = PG_GETARG_BOOL(1);
	Oid			dbid = current_db_only ? MyDatabaseId : InvalidOid;
	RunningTransactions running;
	PGPROC	   *proc;
	TransactionId xid = InvalidTransactionId;
	bool		found = false;

	running = GetRunningTransactionData(dbid);

	proc = BackendPidGetProcWithLock(pid);
	if (proc != NULL)
		xid = ProcGlobal->xids[proc->pgxactoff];

	if (TransactionIdIsValid(xid))
	{
		for (int i = 0; i < running->xcnt; i++)
		{
			if (running->xids[i] == xid)
			{
				found = true;
				break;
			}
		}
	}

	LWLockRelease(ProcArrayLock);
	LWLockRelease(XidGenLock);

	PG_RETURN_BOOL(found);
}

/*
 * SQL function for exposing GetRunningTransactionData()->latestCompletedXid
 * to SQL tests.
 */
PG_FUNCTION_INFO_V1(injection_points_running_xacts_latest_completed_xid);
Datum
injection_points_running_xacts_latest_completed_xid(PG_FUNCTION_ARGS)
{
	bool		current_db_only = PG_GETARG_BOOL(0);
	Oid			dbid = current_db_only ? MyDatabaseId : InvalidOid;
	RunningTransactions running;

	running = GetRunningTransactionData(dbid);

	LWLockRelease(ProcArrayLock);
	LWLockRelease(XidGenLock);

	PG_RETURN_FULLTRANSACTIONID(
		FullTransactionIdFromU64((uint64) running->latestCompletedXid));
}

/*
 * SQL function for dropping an injection point.
 */
PG_FUNCTION_INFO_V1(injection_points_detach);
Datum
injection_points_detach(PG_FUNCTION_ARGS)
{
	char	   *name = text_to_cstring(PG_GETARG_TEXT_PP(0));

	if (!InjectionPointDetach(name))
		elog(ERROR, "could not detach injection point \"%s\"", name);

	/* Remove point from local list, if required */
	if (inj_list_local != NIL)
	{
		MemoryContext oldctx;

		oldctx = MemoryContextSwitchTo(TopMemoryContext);
		inj_list_local = list_delete(inj_list_local, makeString(name));
		MemoryContextSwitchTo(oldctx);
	}

	PG_RETURN_VOID();
}

/*
 * SQL function for listing all the injection points attached.
 */
PG_FUNCTION_INFO_V1(injection_points_list);
Datum
injection_points_list(PG_FUNCTION_ARGS)
{
#define NUM_INJECTION_POINTS_LIST 3
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	List	   *inj_points;
	ListCell   *lc;

	/* Build a tuplestore to return our results in */
	InitMaterializedSRF(fcinfo, 0);

	inj_points = InjectionPointList();

	foreach(lc, inj_points)
	{
		Datum		values[NUM_INJECTION_POINTS_LIST];
		bool		nulls[NUM_INJECTION_POINTS_LIST];
		InjectionPointData *inj_point = lfirst(lc);

		memset(values, 0, sizeof(values));
		memset(nulls, 0, sizeof(nulls));

		values[0] = PointerGetDatum(cstring_to_text(inj_point->name));
		values[1] = PointerGetDatum(cstring_to_text(inj_point->library));
		values[2] = PointerGetDatum(cstring_to_text(inj_point->function));

		/* shove row into tuplestore */
		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}

	return (Datum) 0;
#undef NUM_INJECTION_POINTS_LIST
}

void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
		return;

	RegisterShmemCallbacks(&injection_shmem_callbacks);
}
