#include "postgres.h"

#include "catalog/pg_type.h"

#include "replication/logical.h"
#include "replication/origin.h"

#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"

PG_MODULE_MAGIC;

extern void _PG_init(void);

extern void _PG_output_plugin_init(OutputPluginCallbacks *cb);

typedef struct {
    MemoryContext context;
    bool include_xids;
    bool include_timestamp;
    bool skip_empty_xacts;
    bool xact_wrote_changes;
    bool only_local;
    bool include_messages;
    bool include_toast_datum;
    TransactionId xid;
    TimestampTz commit_time;
} JsonDecodingData;

/*
 * Callback Methods.
 */
static void pg_decode_startup(LogicalDecodingContext *ctx,
                              OutputPluginOptions *opt,
                              bool is_init);

static void pg_decode_begin(LogicalDecodingContext *ctx,
                            ReorderBufferTXN *txn);

static void pg_decode_change(LogicalDecodingContext *ctx,
                             ReorderBufferTXN *txn, Relation rel,
                             ReorderBufferChange *change);

static void pg_decode_commit(LogicalDecodingContext *ctx,
                             ReorderBufferTXN *txn,
                             XLogRecPtr commit_lsn);

static bool pg_decode_filter(LogicalDecodingContext *ctx,
                             RepOriginId origin_id);

static void pg_decode_shutdown(LogicalDecodingContext *ctx);

/*
 * Helper Methods.
 */
static void tuple_to_json_fields(StringInfo s,
                                 TupleDesc tupdesc,
                                 HeapTuple tuple,
                                 bool skip_nulls,
                                 bool include_toast_datum);

static void reportErrorInvalidParam(DefElem *elem);

static void reportUnknownParam(DefElem *elem);

static bool hasParameter(DefElem *elem,
                         char *param);

static void pg_output_begin(LogicalDecodingContext *ctx,
                            JsonDecodingData *data,
                            ReorderBufferTXN *txn,
                            bool last_write);

static bool isSystemColumn(Form_pg_attribute attr);

static bool isColumnDeleted(Form_pg_attribute attr);

/*
 * Implementation.
 */

void _PG_init(void) {
}

/*
 * Plugin Configuration Callbacks.
 */

void _PG_output_plugin_init(OutputPluginCallbacks *cb) {
    AssertVariableIsOfType(&_PG_output_plugin_init, LogicalOutputPluginInit);

    cb->startup_cb = pg_decode_startup;
    cb->begin_cb = pg_decode_begin;
    cb->change_cb = pg_decode_change;
    cb->commit_cb = pg_decode_commit;
    cb->filter_by_origin_cb = pg_decode_filter;
    cb->shutdown_cb = pg_decode_shutdown;
}

/*
 *  Callback Implementations
 */
static void pg_decode_startup(LogicalDecodingContext *ctx, OutputPluginOptions *opt, bool is_init) {
    ListCell *option;
    JsonDecodingData *data;
    bool has_parser_error;

    data = palloc0(sizeof(JsonDecodingData));
    data->context = AllocSetContextCreate(ctx->context, "json decoding conversion context", ALLOCSET_DEFAULT_SIZES);
    data->include_xids = true;
    data->include_timestamp = true;
    data->skip_empty_xacts = true;
    data->only_local = false;
    data->include_messages = false;
    data->include_toast_datum = true;

    ctx->output_plugin_private = data;

    opt->output_type = OUTPUT_PLUGIN_TEXTUAL_OUTPUT;
    opt->receive_rewrites = false;

    foreach(option, ctx->output_plugin_options)
    {
        DefElem *elem = lfirst(option);
        has_parser_error = false;
        Assert(elem->arg == NULL || IsA(elem->arg, String));

        if (hasParameter(elem, "include-xids") && elem->arg != NULL) {

            has_parser_error = !parse_bool(strVal(elem->arg), &data->include_xids);
        } else if (hasParameter(elem, "include-timestamp") && elem->arg != NULL) {

            has_parser_error = !parse_bool(strVal(elem->arg), &data->include_timestamp);
        } else if (hasParameter(elem, "skip-empty-xacts") && elem->arg != NULL) {

            has_parser_error = !parse_bool(strVal(elem->arg), &data->skip_empty_xacts);
        } else if (hasParameter(elem, "only-local")) {

            if (elem->arg != NULL) {
                data->only_local = true;
            } else {
                has_parser_error = !parse_bool(strVal(elem->arg), &data->only_local);
            }

        } else if (hasParameter(elem, "include-rewrites") && elem->arg != NULL) {

            has_parser_error = !parse_bool(strVal(elem->arg), &opt->receive_rewrites);
        } else if (hasParameter(elem, "include-toast-datum") && elem->arg != NULL) {

            has_parser_error = !parse_bool(strVal(elem->arg), &data->include_toast_datum);
        } else {
            reportUnknownParam(elem);
        }

        if (has_parser_error) {
            reportErrorInvalidParam(elem);
        }
    }
}

static void pg_decode_begin(LogicalDecodingContext *ctx, ReorderBufferTXN *txn) {
    JsonDecodingData *data = ctx->output_plugin_private;

    data->xact_wrote_changes = false;

    if (data->skip_empty_xacts)
        return;

    pg_output_begin(ctx, data, txn, true);
}


static void pg_decode_change(LogicalDecodingContext *ctx,
                             ReorderBufferTXN *txn,
                             Relation relation,
                             ReorderBufferChange *change) {
    JsonDecodingData *data;
    Form_pg_class class_form;
    TupleDesc tupdesc;
    MemoryContext old;

    data = ctx->output_plugin_private;

    /* output BEGIN if we haven't yet */
    if (data->skip_empty_xacts && !data->xact_wrote_changes) {
        pg_output_begin(ctx, data, txn, false);
    }
    data->xact_wrote_changes = true;

    class_form = RelationGetForm(relation);
    tupdesc = RelationGetDescr(relation);

    /* Avoid leaking memory by using and resetting our own context */
    old = MemoryContextSwitchTo(data->context);

    OutputPluginPrepareWrite(ctx, true);

    appendStringInfoString(ctx->out, "{ \"pg_change_table\": \"");
    appendStringInfoString(ctx->out,
                           quote_qualified_identifier(
                                   get_namespace_name(
                                           get_rel_namespace(RelationGetRelid(relation))),
                                   class_form->relrewrite ? get_rel_name(class_form->relrewrite) :
                                   NameStr(class_form->relname)));

    appendStringInfoString(ctx->out, "\", ");

    if (data->include_timestamp) {
        appendStringInfo(ctx->out, "\"pg_change_tnx_time\": \"%s\", ", timestamptz_to_str(txn->commit_time));
    }

    if (data->include_xids) {
        appendStringInfo(ctx->out, "\"pg_change_tnx_id\": %u, ", txn->xid);
    }

    appendStringInfoString(ctx->out, " \"pg_change_type\": ");

    switch (change->action) {
        case REORDER_BUFFER_CHANGE_INSERT:
            appendStringInfoString(ctx->out, "\"INSERT\", ");
            if (change->data.tp.newtuple != NULL) {
                tuple_to_json_fields(ctx->out,
                                     tupdesc,
                                     &change->data.tp.newtuple->tuple,
                                     false,
                                     data->include_toast_datum);
            }
            break;
        case REORDER_BUFFER_CHANGE_UPDATE:
            appendStringInfoString(ctx->out, "\"UPDATE\", ");

            if (change->data.tp.oldtuple != NULL) {
                appendStringInfoString(ctx->out, " \"old_primary_key\": { ");
                tuple_to_json_fields(ctx->out, tupdesc,
                                     &change->data.tp.oldtuple->tuple,
                                     true,
                                     data->include_toast_datum);
                appendStringInfoString(ctx->out, " }, ");
            }

            if (change->data.tp.newtuple != NULL) {
                tuple_to_json_fields(ctx->out,
                                     tupdesc,
                                     &change->data.tp.newtuple->tuple,
                                     false,
                                     data->include_toast_datum);
            }

            break;
        case REORDER_BUFFER_CHANGE_DELETE:
            appendStringInfoString(ctx->out, "\"DELETE\", ");

            if (change->data.tp.oldtuple != NULL) {
                tuple_to_json_fields(ctx->out,
                                     tupdesc,
                                     &change->data.tp.oldtuple->tuple,
                                     true,
                                     data->include_toast_datum);
            }

            break;
        default:
            Assert(false);
    }
    appendStringInfoString(ctx->out, " }");

    MemoryContextSwitchTo(old);
    MemoryContextReset(data->context);

    OutputPluginWrite(ctx, true);
}

static void pg_decode_commit(LogicalDecodingContext *ctx, ReorderBufferTXN *txn, XLogRecPtr commit_lsn) {

}

static bool pg_decode_filter(LogicalDecodingContext *ctx, RepOriginId origin_id) {
    JsonDecodingData *data = ctx->output_plugin_private;

    return data->only_local && origin_id != InvalidRepOriginId;
}

static void pg_decode_shutdown(LogicalDecodingContext *ctx) {
    JsonDecodingData *data = ctx->output_plugin_private;
    MemoryContextDelete(data->context);
}

/*
 * Helper Implementations.
*/

static void pg_output_begin(LogicalDecodingContext *ctx,
                            JsonDecodingData *data,
                            ReorderBufferTXN *txn,
                            bool last_write) {
    if (data->include_xids) {
        data->xid = txn->xid;
    }
    if (data->include_timestamp) {
        data->commit_time = txn->commit_time;
    }
}


static void print_literal(StringInfo s, Oid typid, char *outputstr) {
    const char *valptr;

    switch (typid) {
        case INT2OID:
        case INT4OID:
        case INT8OID:
        case OIDOID:
        case FLOAT4OID:
        case FLOAT8OID:
        case NUMERICOID:
            appendStringInfoString(s, outputstr);
            break;

        case BITOID:
        case VARBITOID:
            appendStringInfo(s, "\"%s\"", outputstr);
            break;

        case BOOLOID:
            if (strcmp(outputstr, "t") == 0)
                appendStringInfoString(s, "true");
            else
                appendStringInfoString(s, "false");
            break;

        default:
            appendStringInfoChar(s, '"');
            for (valptr = outputstr; *valptr; valptr++) {
                char ch = *valptr;

                if (SQL_STR_DOUBLE(ch, false))
                    appendStringInfoChar(s, ch);
                appendStringInfoChar(s, ch);
            }
            appendStringInfoChar(s, '"');
            break;
    }
}

static void tuple_to_json_fields(StringInfo s,
                                 TupleDesc tupdesc,
                                 HeapTuple tuple,
                                 bool skip_nulls,
                                 bool include_toast_datum) {
    int natt;

    for (natt = 0; natt < tupdesc->natts; natt++) {
        Form_pg_attribute attr;
        Oid typid;
        Oid typoutput;
        bool typisvarlena;
        Datum origval;
        bool isnull;

        attr = TupleDescAttr(tupdesc, natt);

        if (isColumnDeleted(attr) || isSystemColumn(attr)) {
            continue;
        }

        typid = attr->atttypid;

        /* get Datum from tuple */
        origval = heap_getattr(tuple, natt + 1, tupdesc, &isnull);

        if (isnull && skip_nulls) {
            continue;
        }

        if (natt > 0) {
            appendStringInfoChar(s, ',');
        }

        appendStringInfoString(s, " \"");
        appendStringInfoString(s, quote_identifier(NameStr(attr->attname)));
        appendStringInfoString(s, "\"");

        /* query output function */
        getTypeOutputInfo(typid, &typoutput, &typisvarlena);

        /* print separator */
        appendStringInfoChar(s, ':');
        appendStringInfoChar(s, ' ');

        /* print data */
        if (isnull) {
            appendStringInfoString(s, "null");
        } else if (typisvarlena && VARATT_IS_EXTERNAL_ONDISK(origval) && !include_toast_datum) {
            appendStringInfoString(s, "unchanged-toast-datum");
        } else if (!typisvarlena) {
            print_literal(s, typid, OidOutputFunctionCall(typoutput, origval));
        } else {
            Datum val;    /* definitely detoasted Datum */

            val = PointerGetDatum(PG_DETOAST_DATUM(origval));
            print_literal(s, typid, OidOutputFunctionCall(typoutput, val));
        }

    }
}

void reportErrorInvalidParam(DefElem *elem) {
    ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("could not parse value \"%s\" for parameter \"%s\"",
                           strVal(elem->arg), elem->defname)));
}

void reportUnknownParam(DefElem *elem) {
    ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("option \"%s\" = \"%s\" is unknown",
                           elem->defname,
                           elem->arg ? strVal(elem->arg) : "(null)")));
}

bool hasParameter(DefElem *elem, char *param) {
    return strcmp(elem->defname, param) == 0;
}

bool isSystemColumn(Form_pg_attribute attr) {
    return attr->attnum < 0;
}

bool isColumnDeleted(Form_pg_attribute attr) {
    return attr->attisdropped;
}