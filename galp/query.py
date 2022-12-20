"""
Implementation of complex queries within the command asynchronous system
"""

from .graph import TaskType
from .commands import Command, Status

def run_task(script, task, dry=False):
    """
    Creates command appropriate to type of task (query or non-query)
    """
    if hasattr(task, 'query'):
        if dry:
            raise NotImplementedError('Cannot dry-run queries yet')
        return Query(script, task.subject.name, task.query)

    if dry:
        return script.do_once('DRYRUN', task.name)
    return script.do_once('RUN', task.name)

class Query(Command):
    """
    Local execution of a query

    Args:
        subject: the name of the task to apply the query to
        query: the native object representing the query
    """
    def __init__(self, script, subject, query):
        self.subject = subject
        self.query = query
        super().__init__(script)

    def __str__(self):
        return f'query on {self.subject} {self.query}'

    def _init(self):
        """
        Process the query and decide on further commands to issue
        """

        # Simple queries
        # ==============

        # Query terminator, this is a query leaf and requests the task result
        # itself ; issue a RUN
        if self.query is True:
            return '_run', self.do_once('RUN', self.subject)

        # Status of task, issue a STAT command
        if self.query in ('$done', ('$done', True)):
            return '_status', self.do_once('STAT', self.subject)

        # Definition of task, issue a STAT command
        if self.query in ('$def', ('$def', True)):
            return '_def', self.do_once('STAT', self.subject)

        # Base of task, issue a SRUN
        if self.query in ('$base', ('$base', True)):
            return '_base', self.do_once('SRUN', self.subject)

        # Recursive queries
        # ================

        # Query set, turn each in its own query
        if hasattr(self.query, 'items'):
            return '_queryset', [
                    Query(self.script, self.subject, (key, value))
                    for key, value in self.query.items()
                    ]

        # Query items
        try:
            query_key, _sub_query = self.query
        except (TypeError, ValueError):
            raise NotImplementedError(self.query) from None
        if not isinstance(query_key, str):
            raise NotImplementedError(self.query)

        # Arg query, get the task definition first
        if query_key == '$args':
            return '_args', self.do_once('STAT', self.subject)

        # Children query, run the task first
        if query_key == '$children':
            return '_children', self.do_once('SSUBMIT', self.subject)

        # Iterator, get the raw object first
        if query_key == '*':
            return '_iterator', self.do_once('SRUN', self.subject)

        # Direct indexing, get raw object first
        if query_key.isidentifier() or query_key.isnumeric():
            return '_index', self.do_once('SRUN', self.subject)

        raise NotImplementedError(self.query)

    def _status(self, stat_cmd):
        """
        Check and returns status request
        """
        task_done, *_ = stat_cmd.result

        self.result = task_done
        return Status.DONE

    def _def(self, stat_cmd):
        """
        Check and returns definition request
        """
        _done, task_dict, _children = stat_cmd.result

        self.result = task_dict
        return Status.DONE

    def _args(self, stat_cmd):
        """
        Build list of sub-queries for arguments of subject task from the
        definition obtained from STAT
        """
        _task_done, task_dict, _children = stat_cmd.result
        _, arg_subqueries = self.query

        # bare list of indexes, set the subquery to the whole object
        if not hasattr(arg_subqueries, 'items'):
            arg_subqueries = { index: True for index in arg_subqueries }

        if not 'arg_names' in task_dict:
            raise TypeError('Object is not a job-type Task, cannot use a "args"'
                ' query here.')

        sub_commands = []
        for index, subquery in arg_subqueries.items():
            try:
                num_index = int(index)
                target = task_dict['arg_names'][num_index]
            except ValueError: # keyword argument
                target = task_dict['kwarg_names'][index]
            sub_commands.append(
                Query(self.script, target, subquery)
                )
        return '_dict_queryset', sub_commands

    def _children(self, ssub_cmd):
        """
        Build a list of sub-queries for children of subject task
        from the children list obtained from SRUN
        """
        children = ssub_cmd.result

        _, child_subqueries = self.query
        sub_commands = []

        if '*' in child_subqueries:
            if len(child_subqueries) > 1:
                raise NotImplementedError('Only one subquery supported when '
                        'using universal children queries at the moment')
            child_subqueries = { i: child_subqueries['*']
                    for i in range(len(children))
                    }

        for index, subquery in child_subqueries.items():
            try:
                num_index = int(index)
                target = children[num_index]
            except ValueError: # key-indexed child
                raise NotImplementedError(index) from None
            sub_commands.append(
                Query(self.script, target, subquery)
                )
        return '_dict_queryset', sub_commands

    def _dict_queryset(self, *query_items):
        """
        Check and merge argument query items
        """
        _, arg_subqueries = self.query
        if '*' in arg_subqueries:
            arg_subqueries = range(len(query_items))

        result = {}
        for index, qitem in zip(arg_subqueries, query_items):
            result[str(index)] = qitem.result
        self.result = result

        return Status.DONE

    def _queryset(self, *query_items):
        """
        Check and merge query items
        """
        result = {}
        for qitem in query_items:
            result[qitem.query[0]] = qitem.result
        self.result = result
        return Status.DONE

    def _iterator(self, _):
        """
        Check simple run, extract raw result and build subqueries
        """
        shallow_obj = self.script.store.get_native(self.subject, shallow=True)

        _, subquery = self.query
        subquery_commands = []

        for task in shallow_obj:
            if not isinstance(task, TaskType):
                raise TypeError('Object is not a collection of tasks, cannot'
                    ' use a "*" query here')
            subquery_commands.append(
                    Query(self.script, task.name, subquery)
                    )

        return '_items', subquery_commands

    def _items(self, *items):
        """
        Check and pack items
        """
        self.result = [ item.result for item in items ]
        return Status.DONE

    def _index(self, _):
        """
        Subquery for a simple item
        """
        shallow_obj = self.script.store.get_native(self.subject, shallow=True)

        index, subquery = self.query

        try:
            task = shallow_obj[int(index) if index.isnumeric() else index]
        except Exception as exc:
            raise RuntimeError(
                f'Query failed, cannot access item "{index}" of task {self.subject}'
                ) from exc

        if not isinstance(task, TaskType):
            raise TypeError(
                f'Item "{index}" of task {self.subject} is not itself a '
                f'task, cannot apply subquery "{subquery}" to it'
                )

        return '_item', Query(self.script, task.name, subquery)

    def _item(self, item):
        """
        Return result of simple indexed command
        """
        self.result = item.result
        return Status.DONE


    def _base(self, _):
        """
        Return base result
        """
        self.result = self.script.store.get_native(self.subject, shallow=True)
        return Status.DONE

    def _run(self, _):
        """
        Return full result
        """
        self.result = self.script.store.get_native(self.subject, shallow=False)
        return Status.DONE
