"""Tools for working with veritable-python.

See also: https://dev.priorknowledge.com/docs/client/python

"""

import os
import sys
import time
from .cursor import Cursor
from .connection import Connection
from .exceptions import VeritableError
from .utils import (_make_table_id, _make_analysis_id, _check_id,
    _format_url, _handle_unicode_id)

# ensure map returns an iterator (as in python 3) not a generator (as in 2)
try:
    from future_builtins import map
except ImportError:
    try:
        from itertools import imap as map
    except ImportError:
        pass


BASE_URL = "https://api.priorknowledge.com/"


def connect(api_key=None, api_base_url=None, ssl_verify=True,
        enable_gzip=True, debug=False):
    """Entry point to the Veritable API.

    Returns a veritable.api.API instance.

    Arguments:
    api_key -- the API key to use for access. (default: None) If None, reads
        the API key in from the VERITABLE_KEY environment variable.
    api_base_url -- the base url of the API. (default: None) If None, reads
        the url in from the VERITABLE_URL environment variable, and if
        nothing is found, uses https://api.priorknowledge.com by default.
    ssl_verify -- controls whether SSL keys are verified. (default: True)
    enable_gzip -- controls whether requests to and from the API server are
        gzipped. (default: True)
    debug -- controls the production of debug messages. (default: False)

    See also: https://dev.priorknowledge.com/docs/client/python

    """
    if api_key is None:
        api_key = os.getenv("VERITABLE_KEY")
    if api_base_url is None:
        api_base_url = os.getenv("VERITABLE_URL") or BASE_URL
    connection = Connection(api_key=api_key, api_base_url=api_base_url,
            ssl_verify=ssl_verify, enable_gzip=enable_gzip, debug=debug)
    try:
        connection_test = connection.get("/")
    except Exception as e:
        raise VeritableError("Error connecting to server: No Veritable " \
        "server found at {0} using API key {1}".format(api_base_url,
            api_key), internal=e, internal_traceback=sys.exc_info()[2])
    try:
        status = connection_test['status']
        entropy = connection_test['entropy']
    except:
        raise VeritableError("Error connecting to server: No Veritable " \
        "server found at {0} using API key {1}".format(api_base_url,
            api_key))
    if status != "SUCCESS" or not isinstance(entropy, float):
        raise VeritableError("Error connecting to server: No Veritable " \
        "server found at {0} using API key {1}".format(api_base_url,
            api_key))
    return API(connection)


class API:

    """Represents the resources available to a user of the Veritable API.

    Methods:
    table_exists -- checks whether a table with a given id is available.
    get_tables -- gets the collection of available tables.
    get_table -- gets a table with a given id.
    create_table -- creates a new table.
    delete_table -- deletes a table with a given id.

    See also: https://dev.priorknowledge.com/docs/client/python

    """

    def __init__(self, connection):
        """Initializes the Veritable API.

        Users should not invoke directly -- use veritable.connect as the
        entry point instead.

        Arguments:
        connection -- a veritable.connection.Connection object

        See also: https://dev.priorknowledge.com/docs/client/python

        """
        self._conn = connection
        self._url = connection.api_base_url

    def __str__(self):
        return "<veritable.API url='" + self._url + "'>"

    def __repr__(self):
        return self.__str__()

    def _link(self, name):
        # Retrieves a subresource by name
        if name not in self._doc['links']:
            raise VeritableError("API instance is missing link "\
            "to {0}".format(name))
        return self._doc['links'][name]

    def limits(self):
        """Retrieves the current API limits as a dict.

        See also: https://dev.priorknowledge.com/docs/client/python

        """
        return self._conn.get(_format_url(["user", "limits"]))

    def table_exists(self, table_id):
        """Checks if a table with the specified id is available to the user.

        Returns True if the table is available, False otherwise.

        Arguments:
        table_id -- the string id of the table to check.

        See also: https://dev.priorknowledge.com/docs/client/python

        """
        try:
            self.get_table(table_id)
        except:
            return False
        else:
            return True

    def get_tables(self, start=None, limit=None):
        """Gets the tables available to the user.

        Returns an iterator over veritable.api.Table objects.

        Arguments:
        start -- The table id from which to start (default: None). Tables
          whose id fields are greater than or equal to start in lexicographic
          order will be returned by the iterator. If None, iteration will
          start at the lexicographically first id.
        limit -- If set to an integer value, will limit the number of tables
          returned by the iterator (default: None). If None, the number of
          tables returned will not be limited.

        See also: https://dev.priorknowledge.com/docs/client/python

        """
        cursor = Cursor(
                self._conn,
                _format_url(["tables"]),
                start=start,
                limit=limit)

        return map(lambda t: Table(self._conn, t), cursor)

    def get_table(self, table_id):
        """Gets a table from the collection by its id.

        Returns a veritable.api.Table instance.

        Arguments:
        table_id -- the string id of the table to get.

        See also: https://dev.priorknowledge.com/docs/client/python

        """
        r = self._conn.get(_format_url(["tables", table_id]))
        return Table(self._conn, r)

    def create_table(self, table_id=None, description="", force=False):
        """Creates a new table.

        Returns a veritable.api.Table instance.

        Arguments:
        table_id -- the string id of the table to create (default: None)
            Must contain only alphanumerics, underscores, and hyphens.
            If None, create_table will autogenerate a new id for the table.
        description -- the string description of the table to create
            (default: '')
        force -- controls whether create_table will overwrite an existing
            table with the same id (default: False)

        See also: https://dev.priorknowledge.com/docs/client/python

        """
        if table_id is None:
            autogen = True
            table_id = _make_table_id()
        else:
            table_id = _handle_unicode_id(table_id)
            _check_id(table_id)
            autogen = False
        if self.table_exists(table_id):
            if autogen:
                return self.create_table(table_id=None,
                            description=description, force=False)
            if not force:
                raise VeritableError("Can't create table with id {0}: " \
                "table already exists. Set force=True to " \
                "override.".format(table_id))
            else:
                self.delete_table(table_id)
        r = self._conn.post("tables",
                data={"_id": table_id, "description": description})
        return Table(self._conn, r)

    def delete_table(self, table_id):
        """Deletes a table from the collection by its id.

        Returns None on success. Silently succeeds on attempts to delete
        nonexistent resources.

        Arguments:
        table_id -- the string id of the table to delete.

        See also: https://dev.priorknowledge.com/docs/client/python

        """
        self._conn.delete(_format_url(["tables", table_id]))


class Table:

    """Represents the resources associated with a single table.

    Instance Attributes:
    id -- the string id of the table

    Methods:
    delete -- deletes the table resource.
    get_row -- gets a row from the table by its id
    get_rows -- gets all the rows of the table
    upload_row -- uploads a row to the table
    batch_upload_rows -- uploads a list of rows to the table
    delete_row -- deletes a row from the table
    batch_delete_rows -- deletes a list of rows from the table
    get_analyses -- gets all the analyses of the table
    get_analysis -- gets an analysis from the table by its id
    delete_analysis -- deletes an analysis of the table by its id
    create_analysis -- creates a new analysis of the table

    See also: https://dev.priorknowledge.com/docs/client/python

    """

    def __init__(self, connection, doc):
        """Initializes a Veritable Table.

        Users should not invoke directly -- use veritable.connect as the
        entry point instead.

        Arguments:
        connection -- a veritable.connection.Connection object
        doc - the Python object translation of the resource's JSON doc

        See also: https://dev.priorknowledge.com/docs/client/python

        """
        self._conn = connection
        self._doc = doc

    def __str__(self):
        return "<veritable.Table id='" + self.id + "'>"

    def __repr__(self):
        return self.__str__()

    def _link(self, name):
        # Retrieves a subresource by name
        if name not in self._doc['links']:
            raise VeritableError("Table instance is missing link " \
            "to {0}".format(name))
        return self._doc['links'][name]

    def _analysis_exists(self, analysis_id):
        # Checks if an analysis with a given id already exists.
        try:
            self.get_analysis(analysis_id)
        except:
            return False
        else:
            return True

    @property
    def id(self):
        """The string id of the table.

        See also: https://dev.priorknowledge.com/docs/client/python

        """
        return str(self._doc['_id'])

    def delete(self):
        """Deletes the table resource.

        Returns None on success. Silently succeeds on attempts to delete
        nonexistent resources.

        See also: https://dev.priorknowledge.com/docs/client/python

        """
        self._conn.delete(self._link("self"))

    def get_row(self, row_id):
        """Gets a row from the table by its id.

        Returns a dict representing the values in the row.

        Arguments:
        row_id -- the string id of the row to fetch

        See also: https://dev.priorknowledge.com/docs/client/python

        """
        return self._conn.get(_format_url([self._link("rows"), row_id],
            noquote=[0]))

    def get_rows(self, start=None, limit=None):
        """Gets the rows of the table.

        Returns an iterator over the rows of the table.

        Arguments:
        start -- The row id from which to start (default: None). Rows whose id
          fields are greater than or equal to start in lexicographic order
          will be returned by the iterator. If None, iteration will start at
          the lexicographically first id.
        limit -- If set to an integer value, will limit the number of rows
          returned by the iterator (default: None). If None, the number of
          rows returned will not be limited.

        See also: https://dev.priorknowledge.com/docs/client/python

        """
        collection = self._link("rows")
        return Cursor(self._conn, collection, start=start,
            limit=limit)

    def upload_row(self, row):
        """Adds a row to the table or updates an existing row.

        Returns None on success.

        Arguments:
        row -- a dict representing the row to upload. Must contain an '_id'
            key whose value is a string containing only alphanumerics,
            underscores, and hyphens, and is unique in the table.

        See also: https://dev.priorknowledge.com/docs/client/python

        """
        if not isinstance(row, dict):
            raise VeritableError("Must provide a row dict to upload.")
        if "_id" not in row:
            raise VeritableError("Rows must contain row ids in the _id " \
            "field.")
        else:
            row_id = _handle_unicode_id(row["_id"])
            _check_id(row_id)
        self._conn.put(_format_url([self._link("rows"), row_id], noquote=[0]),
            row)

    def batch_upload_rows(self, rows, per_page=100):
        """Batch adds rows to the table or updates existing rows.

        By default, paginates requests in chunks of 100 rows. This
        parameter can be adjusted.

        Returns None on success.

        Arguments:
        rows - a list of dicts representing the rows to upload. Each dict
            must contain an '_id' key whose value is a string containing only
            alphanumerics, underscores, and hyphens, and is unique in the
            table.
        per_page - the number of rows to upload per HTTP request (default: 100)

        See also: https://dev.priorknowledge.com/docs/client/python

        """
        self._batch_modify_rows('put', rows, per_page)

    def _batch_modify_rows(self, action, rows, per_page):
        rs = []
        if not isinstance(per_page, int) or not per_page > 0:
            raise VeritableError("Page size must be an int greater than 0")
        for row in rows:
            if not isinstance(row, dict):
                raise VeritableError("Rows must be represented by row dicts.")
            if not "_id" in row:
                raise VeritableError("Rows must contain row ids in the _id "\
                "field.")
            row["_id"] = _handle_unicode_id(row["_id"])
            _check_id(row["_id"])
            rs.append(row)
        batch = []
        i = 0
        for r in rs:
            batch.append(r)
            i = i+1
            if i == per_page:
                data = {'action': action, 'rows': batch}
                self._conn.post(self._link('rows'), data)
                i = 0
                batch = []
        if len(batch) > 0:
            data = {'action': 'put', 'rows': batch}
            self._conn.post(self._link('rows'), data)

    def delete_row(self, row_id):
        """Deletes a row from the table by its id.

        Returns None on success. Silently succeeds on attempts to delete
        nonexistent resources.

        Arguments:
        row_id -- the string id of the row to delete.

        See also: https://dev.priorknowledge.com/docs/client/python

        """
        self._conn.delete(_format_url([self._link("rows"), row_id],
            noquote=[0]))

    def batch_delete_rows(self, rows, per_page=100):
        """Batch deletes rows from the table.

        Returns None on success. Silently succeeds on attempts to delete
        nonexistent resources.

        Arguments:
        rows -- a list of dics representing the rows to delete. Each dict
            must contain an '_id' key whose value is the string id of a row
            to delete from the table, and need not contain any other keys.

        See also: https://dev.priorknowledge.com/docs/client/python

        """
        self._batch_modify_rows('delete', rows, per_page)

    def get_analyses(self, start=None, limit=None):
        """Gets the analyses of the table.

        Returns an iterator over veritable.api.Analysis objects.

        Arguments:
        start -- The analysis id from which to start (default: None).
          Analyses whose id fields are greater than or equal to start in
          lexicographic order will be returned by the iterator. If None,
          iteration will start at the lexicographically first id.
        limit -- If set to an integer value, will limit the number of
          analyses returned by the iterator (default: None). If None, the
          number of analyses returned will not be limited.

        See also: https://dev.priorknowledge.com/docs/client/python

        """
        cursor = Cursor(
                self._conn,
                self._link("analyses"),
                start=start,
                limit=limit)

        return map(lambda a: Analysis(self._conn, a), cursor)

    def get_analysis(self, analysis_id):
        """Gets an analysis of the table by its id.

        Returns a veritable.api.Analysis instance.

        Arguments:
        analysis_id -- the string id of the analysis to fetch.

        See also: https://dev.priorknowledge.com/docs/client/python

        """
        r = self._conn.get(_format_url([self._link("analyses"), analysis_id],
            noquote=[0]))
        return Analysis(self._conn, r)

    def delete_analysis(self, analysis_id):
        """Deletes an analysis of the table by its id.

        Returns None on success. Silently succeeds on attempts to delete
        nonexistent resources.

        Arguments:
        analysis_id -- the string id of the analysis to delete

        See also: https://dev.priorknowledge.com/docs/client/python

        """
        self._conn.delete(_format_url([self._link("analyses"), analysis_id],
            noquote=[0]))

    def create_analysis(self, schema, analysis_id=None, description="",
                        type="veritable", force=False):
        """Creates a new analysis of the table.

        Returns a veritable.api.Analysis instance.

        Arguments:
        analysis_id -- the string id of the analysis to create (default: None)
            Must contain only alphanumerics, underscores, and hyphens.
            If None, create_analysis will autogenerate a new id for the table.
        schema -- the analysis schema to use (default: None) The schema must
            be a Python dict of the form:
                {'col_1': {type: 'datatype'}, 'col_2': {type: 'datatype'}, ...}
            where the specified datatype for each column one of ['real',
            'boolean', 'categorical', 'count'] and is valid for the column.
        description -- the string description of the analysis to create
            (default: '')
        type -- type of analysis; must be "veritable" (default: 'veritable')
        force -- controls whether create_analysis will overwrite an existing
            analysis with the same id (default: False)

        See also: https://dev.priorknowledge.com/docs/client/python

        """
        if type != "veritable":
            raise VeritableError("Invalid analysis type.")
        if analysis_id is None:
            autogen = True
            analysis_id = _make_analysis_id()
        else:
            analysis_id = _handle_unicode_id(analysis_id)
            _check_id(analysis_id)
            autogen = False
        if self._analysis_exists(analysis_id):
            if autogen:
                return self.create_analysis(schema=schema,
                        description=description, analysis_id=None,
                        type=type, force=False)
            if not force:
                raise VeritableError("Can't create analysis with id {0}: " \
                "analysis already exists. Set force=True to " \
                "override.".format(analysis_id))
            else:
                self.delete_analysis(analysis_id)
        r = self._conn.post(self._link("analyses"),
                data={"_id": analysis_id, "description": description,
                      "type": type, "schema": schema})
        return Analysis(self._conn, r)


class Analysis:

    """Represents an analysis resource.

    Instance Attributes:
    id -- the string id of the table
    state -- the state of the analysis
    error -- the detailed error encountered in analysis, if any

    Methods:
    update -- refreshes the state of the analysis
    delete -- deletes the analysis resource
    get_schema -- gets the schema associated with the analysis
    wait -- waits until the analysis completes
    predict -- makes predictions from the analysis
    related_to -- scores how related other columns are to column of interest

    See also: https://dev.priorknowledge.com/docs/client/python

    """

    def __init__(self, connection, doc):
        self._conn = connection
        self._doc = doc

    def __str__(self):
        return "<veritable.Analysis id='" + self.id + "'>"

    def __repr__(self):
        return self.__str__()

    def _link(self, name):
        if name not in self._doc['links']:
            raise VeritableError("Analysis instance is missing link " \
            "to {0}".format(name))
        return self._doc['links'][name]

    @property
    def id(self):
        """The string id of the analysis.

        See also: https://dev.priorknowledge.com/docs/client/python

        """
        return str(self._doc['_id'])

    @property
    def finished_at(self):
        """The time the analysis completed.

        See also: https://dev.priorknowledge.com/docs/client/python

        """
        return str(self._doc['finished_at'])

    @property
    def created_at(self):
        """The time the analysis was created.

        See also: https://dev.priorknowledge.com/docs/client/python

        """
        return str(self._doc['created_at'])

    @property
    def state(self):
        """The state of the analysis

        A string, one of 'succeeded', 'failed', or 'running'. Run the
        update method to refresh.

        See also: https://dev.priorknowledge.com/docs/client/python

        """
        return str(self._doc['state'])

    @property
    def error(self):
        """The error, if any, encountered by the analysis.

        A Python object with details of the error, or None if the analysis
        has not failed.

        See also: https://dev.priorknowledge.com/docs/client/python

        """
        if self.state != 'failed':
            return None
        else:
            return self._doc['error']

    @property
    def progress(self):
        """An estimate of the time remaining for the analysis to complete.

        If the analysis is still running, returns a dict containing the fields:
        percent -- an integer between 0 and 100 indicating how much of The
          analysis is complete
        finished_at_estimate -- a timestamp representing the estimated time
          at which the analysis will complete

        If the analysis has succeeded or failed, None.

        See also: https://dev.priorknowledge.com/docs/client/python

        """
        if self.state == 'succeeded':
            return self._doc['progress']
        else:
            return None

    def update(self):
        """Refreshes the analysis state

        Checks whether the analysis has succeeded or failed, updating the
        state and error attributes appropriately.

        See also: https://dev.priorknowledge.com/docs/client/python

        """
        self._doc = self._conn.get(self._link('self'))

    def delete(self):
        """Deletes the analysis resource.

        Returns None on success. Silently succeeds on attempts to delete
        nonexistent resources.

        See also: https://dev.priorknowledge.com/docs/client/python

        """
        self._conn.delete(self._link('self'))

    def get_schema(self):
        """Gets the schema of the analysis.

        Returns the Python dict representing the analysis schema.

        See also: https://dev.priorknowledge.com/docs/client/python

        """
        return self._conn.get(self._link('schema'))

    def wait(self, max_time=None, poll=2):
        """Waits for the running analysis to succeed or fail.

        Returns None when the analysis succeeds or fails, and blocks until
        it does. If a timeout is specified, raises a VeritableError if the
        timeout has elapsed without the analysis completing.

        Arguments:
        max_time -- the number of seconds after which to return or raise an
          exception. If this is None, analysis.wait will block indefinitely.
        poll -- the number of seconds to wait between updates (default: 2)

        See also: https://dev.priorknowledge.com/docs/client/python

        """
        elapsed = 0
        while self.state == 'running':
            time.sleep(poll)
            if max_time is not None:
                elapsed += poll
                if elapsed > max_time:
                    raise VeritableError("Maximum time of {0} " \
                    "exceeded".format(max_time))
            self.update()

    def predict(self, row, count=100):
        """Makes predictions from the analysis.

        Returns a veritable.api.Prediction instance.

        Arguments:
        row -- the row dict whose missing values are to be predicted. These
            values should be None in the row argument.
        count -- the number of samples from the joint predictive distribution
            to return. the number of samples allowed by the API is limited on
            a per-user basis.

        See also: https://dev.priorknowledge.com/docs/client/python

        """
        if self.state == 'running':
            self.update()
        if self.state == 'succeeded':
            if not isinstance(row, dict):
                raise VeritableError("Must provide a row dict to make "\
                "predictions!")
            res = self._conn.post(self._link('predict'),
                                  data={'data': row, 'count': count})
            if not isinstance(res, list):
                raise VeritableError("Error making predictions: " \
                "{0}".format(res))
            return Prediction(row, res, self.get_schema())
        elif self.state == 'running':
            raise VeritableError("Analysis with id {0} is still running " \
            "and not yet ready to predict".format(self.id))
        elif self.state == 'failed':
            raise VeritableError("Analysis with id {0} has failed and " \
            "cannot predict: {1}".format(self.id, self.error))

    def related_to(self, column_id, start=None, limit=None):
        """Scores how related columns are to column of interest 

        Returns an iterator over the columns in the table.

        Arguments:
        column_id -- the id of the column of interest.
        start -- The column id from which to start (default: None) Columns whose 
           related scores are greater than or equal to the score of start 
          will be returned by the iterator. If None, all rows will be
          returned.
        limit -- If set to an integer value, will limit the number of columns
          returned by the iterator. (default: None) If None, the number of
          columns returned will not be limited.

        See also: https://dev.priorknowledge.com/docs/client/python

        """
        if self.state == 'running':
            self.update()
        if self.state == 'succeeded':
            collection = self._link('related')+'/'+column_id
            return Cursor(self._conn, collection, start=start, limit=limit)
        elif self.state == 'running':
            raise VeritableError("Analysis with id {0} is still running " \
            "and not yet ready to get relateds".format(self.id))
        elif self.state == 'failed':
            raise VeritableError("Analysis with id {0} has failed and " \
            "cannot get relateds: {1}".format(self.id, self.error))



class Prediction(dict):
    """Represents predictions responses.

    A dictionary whose keys are the columns in the prediction request,
    and whose values are point estimates for those columns. For fixed
    columns, the value is the fixed value. For predicted values, the
    point estimate varies by datatype:
    
    real -- mean
    count -- mean rounded to the nearest integer
    categorical -- mode
    boolean -- mode

    Instance attributes:
    distribution -- the underlying predicted distribution as a list of
      row dicts
    uncertainty -- a dict whose keys are the columns in the prediction
      request and whose values are uncertainty measures associated with
      each point estimate. A higher value indicates greater uncertainty.
      These measures vary by datatype:
          real -- length of 90% credible interval
          count -- length of 90% credible interval
          categorical -- total probability of all non-modal values
          boolean -- probability of the non-modal value
    request -- the original predictions request
    schema -- the schema for the columns in the predictions request

    See also: https://dev.priorknowledge.com/docs/client/python

    """
    def __init__(self, request, distribution, schema):
        self.distribution = distribution
        self.uncertainty = {}
        self.request = request
        self.schema = dict([(k, schema[k]) for k in self.request.keys()])
        for k in self.request.keys():
            if self.request[k] is not None:
                self[k] = self.request[k]
                self.uncertainty[k] = 0.0
            else:
                self[k] = self._point_estimate(k)
                self.uncertainty[k] = self._uncertainty(k)

    def _sorted_values(self, column):
        values = [row[column] for row in self.distribution]
        values.sort()
        return values

    def _counts(self, column):
        counts = {}
        for row in self.distribution:
            counts[row[column]] = counts.get(row[column], 0) + 1
        return counts

    def _freqs(self, counts):
        total = sum(counts.values())
        freqs = dict([(k, float(counts[k]) / total) for k in counts])
        return freqs

    def _point_estimate(self, column):
        col_type = self.schema[column]['type']
        if col_type == 'boolean' or col_type == 'categorical':
            # mode
            counts = self._counts(column)
            max_count = 0
            max_value = None
            for value in counts:
                if counts[value] > max_count:
                    max_count = counts[value]
                    max_value = value
            return max_value
        elif col_type == 'real' or col_type == 'count':
            # mean
            values = [row[column] for row in self.distribution]
            mean = sum(values) / float(len(values))
            if col_type == 'real':
                return mean
            else:
                return int(round(mean))
        else:
            assert False, 'bad column type'

    def _uncertainty(self, column):
        vals = [p[column] for p in self.distribution]
        col_type = self.schema[column]['type']
        N = len(vals)
        if col_type == 'boolean' or col_type == 'categorical':
            e = max(vals, key=vals.count)
            c = 1.0 - (sum([1.0 for v in vals if v == e]) / float(N))
            return float(c)
        elif col_type == 'count' or col_type == 'real':
            r = self.credible_values(column)
            return float(r[1] - r[0])

    def prob_within(self, column, set_spec):
        """
        Calculates the probability a column's value lies within a range.

        Based on the given prediction, calculates the marginal probability
        that the predicted value for the given columns lies within the given
        range.

        Arguments:
        column -- The column for which to calculate probabilities
        set_spec -- A representation of the range for which to calculate
          probabilities. For real and count columns, this is a tuple (start,
          end) representing a closed interval. For boolean and categorical
          columns, this is a list of discrete values.

        See also: https://dev.priorknowledge.com/docs/client/python
        
        """
        col_type = self.schema[column]['type']
        if col_type == 'boolean' or col_type == 'categorical':
            count = 0
            for row in self.distribution:
                if row[column] in set_spec:
                    count += 1
            return float(count) / len(self.distribution)
        elif col_type == 'count' or col_type == 'real':
            count = 0
            mn = set_spec[0]
            mx = set_spec[1]
            for row in self.distribution:
                v = row[column]
                if (mn == None or v >= mn) and (mx == None or v <= mx):
                    count += 1
            return float(count) / len(self.distribution)                
        else:
            assert False, 'bad column type'

    def credible_values(self, column, p=None):
        """
        Calculates a credible range for the value of a column.

        Based on the given prediction, calculates a range within which the
        predicted value for the column lies with the given probability.

        Arguments:
        column -- The column for which to calculate the range
        p -- The desired degree of probability. (default: None) If None, will
          default to 0.5 for boolean and categorical columns, and to 0.90 for
          count and real columns.

        See also: https://dev.priorknowledge.com/docs/client/python

        """
        schema = self.schema
        col_type = schema[column]['type']
        if col_type == 'boolean' or col_type == 'categorical':
            if p is None:
                p = .5
            freqs = self._freqs(self._counts(column))
            sorted_freqs = sorted(freqs.items(), key=lambda x: x[1], reverse=True)
            threshold_freqs = dict([(c, a) for c, a in sorted_freqs if a >= p])
            return threshold_freqs
        elif col_type == 'count' or col_type == 'real':
            # Note: this computes an interval that removes equal probability mass 
            # from each end; a possible alternative would be to return the shorted 
            # interval containing the given amount of mass
            if p is None:
                p = .9
            N = len(self.distribution)
            a = int(round(N * (1. - p) / 2.))
            sorted_values = self._sorted_values(column)
            N = len(sorted_values)
            lo = sorted_values[a]
            hi = sorted_values[N - 1 - a]
            return (lo, hi)
        else:
            assert False, 'bad column type'
