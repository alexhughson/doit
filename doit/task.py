
"""Tasks are the main abstractions managed by doit"""

import os
import sys
import inspect
from collections import OrderedDict
from collections.abc import Callable
from pathlib import PurePath

from .cmdparse import CmdOption, TaskParse, normalize_option
from .exceptions import BaseFail, InvalidTask
from .action import create_action, PythonAction
from .dependency import UptodateCalculator, StorageKey
from .deps import Dependency, FileDependency, TaskDependency


def first_line(doc):
    """extract first non-blank line from text, to extract docstring title"""
    if doc is not None:
        for line in doc.splitlines():
            striped = line.strip()
            if striped:
                return striped
    return ''


class DelayedLoader(object):
    """contains info for delayed creation of tasks from a task-creator

    :ivar creator: reference to task-creator function
    :ivar task_dep: (str) name of task that should be executed before the
                    the loader call the creator function
    :ivar basename: (str) basename used when creating tasks
                   This is used when doit creates new tasks to handle
                   tasks and targets specified on command line
    :ivar target_regex: (str) regex for all targets that this loader tasks
                        will create
    :ivar created: (bool) whether this creator was already executed or not
    """
    def __init__(self, creator, executed=None, target_regex=None, creates=None):
        self.creator = creator
        self.task_dep = executed
        self.basename = None
        self.created = False
        self.target_regex = target_regex
        self.creates = creates[:] if creates else []
        self.regex_groups = OrderedDict()  # task_name:RegexGroup
        self.kwargs = None  # task creator kwargs


# used to indicate that a task had DelayedLoader but was already created
DelayedLoaded = False


class Stream():
    """Control task output stream verbosity

    :ivar verbosity: 0,1,2 see Task.execute

    Priority is given by:
    1) command line -> force_global=True
    2) task value
    3) other config (INI, DOIT_CONFIG)
    """

    def __init__(self, verbosity, force_global=False):
        self.force_global = force_global
        if verbosity is not None:
            self.verbosity = verbosity
        else:
            self.verbosity = Task.DEFAULT_VERBOSITY
            self.force_global = False

    def effective_verbosity(self, task_verbosity):
        """return effective verbosity used on task"""
        if self.force_global:
            return self.verbosity
        elif task_verbosity is not None:
            return task_verbosity
        else:
            return self.verbosity

    @staticmethod
    def _get_out_err(verbosity):
        """return tuple (out, err) streams to be used

        Replace stream with None if stream should be captured

        :param verbosity: (int)
        """
        if verbosity == 0:
            return (None, None)
        elif verbosity == 1:
            return (None, sys.stderr)
        else:
            return (sys.stdout, sys.stderr)


class IOConfig:
    def __init__(self, io_data):
        self.capture = io_data.get('capture', True)

    def __repr__(self):
        return f'IOConfig(capture={self.capture})'


class Task(object):
    """Task

    @ivar name string
    @ivar actions: list - L{BaseAction}
    @ivar clean_actions: list - L{BaseAction}
    @ivar loader (DelayedLoader)
    @ivar teardown (list - L{BaseAction})
    @ivar targets: (list -string)
    @ivar task_dep: (list - string)
    @ivar wild_dep: (list - string) task dependency using wildcard *
    @ivar file_dep: (set - string)
    @ivar calc_dep: (set - string) reference to a task
    @ivar dep_changed (list - string): list of file-dependencies that changed
          (are not up_to_date). this must be set before
    @ivar uptodate: (list - bool/None) use bool/computed value instead of
                                       checking dependencies
    @ivar value_savers (list - callables) that return dicts to be added to
                           task values. Always executed on main process.
                           To be used by `uptodate` implementations.
    @ivar setup_tasks (list - string): references to task-names
    @ivar subtask_of: (string) indicate this task is a subtask of task name
    @ivar has_subtask: (bool) indicate this task has subtasks
    @ivar result: (str) last action "result". used to check task-result-dep
    @ivar values: (dict) values saved by task that might be used by other tasks
    @ivar getargs: (dict) values from other tasks
    @ivar doc: (string) task documentation
    @ivar meta: (dict) extra info from user/plugin not directly used by doit

    @ivar options: (dict) calculated params values (from getargs and taskopt)
    @ivar taskopt: (cmdparse.CmdParse)
    @ivar pos_arg: (str) name of parameter in action to receive positional
                     parameters from command line
    @ivar pos_arg_val: (list - str) list of positional parameters values
    @ivar custom_title: function reference that takes a task object as
                        parameter and returns a string.
    """

    DEFAULT_VERBOSITY = 1
    string_types = (str, )
    # list of valid types/values for each task attribute.
    valid_attr = {'basename': (string_types, ()),
                  'name': (string_types, ()),
                  'actions': ((list, tuple), (None,)),
                  'dependencies': ((list, tuple), ()),
                  'file_dep': ((list, tuple, set), ()),  # legacy dict format
                  'task_dep': ((list, tuple), ()),       # legacy dict format
                  'uptodate': ((list, tuple), ()),
                  'calc_dep': ((list, tuple), ()),
                  'targets': ((list, tuple), ()),
                  'setup': ((list, tuple), ()),
                  'clean': ((list, tuple), (True,)),
                  'teardown': ((list, tuple), ()),
                  'doc': (string_types, (None,)),
                  'params': ((list, tuple,), ()),
                  'pos_arg': (string_types, (None,)),
                  'verbosity': ((), (None, 0, 1, 2,)),
                  'io': ((dict,), (None,)),
                  'getargs': ((dict,), ()),
                  'title': ((Callable,), (None,)),
                  'watch': ((list, tuple), ()),
                  'meta': ((dict,), (None,)),
                  }


    def __init__(self, name, actions, dependencies=(), targets=(),
                 uptodate=(),
                 calc_dep=(), setup=(), clean=(), teardown=(),
                 subtask_of=None, has_subtask=False,
                 doc=None, params=(), pos_arg=None,
                 verbosity=None, io=None, title=None, getargs=None,
                 watch=(), meta=None, loader=None):
        """sanity checks and initialization

        @param params: (list of dict for parameters) see cmdparse.CmdOption
        """

        getargs = getargs or {}  # default
        self.check_attr(name, 'name', name, self.valid_attr['name'])
        self.check_attr(name, 'actions', actions, self.valid_attr['actions'])
        self.check_attr(name, 'dependencies', dependencies,
                        self.valid_attr['dependencies'])
        self.check_attr(name, 'uptodate', uptodate, self.valid_attr['uptodate'])
        self.check_attr(name, 'calc_dep', calc_dep, self.valid_attr['calc_dep'])
        self.check_attr(name, 'targets', targets, self.valid_attr['targets'])
        self.check_attr(name, 'setup', setup, self.valid_attr['setup'])
        self.check_attr(name, 'clean', clean, self.valid_attr['clean'])
        self.check_attr(name, 'teardown', teardown, self.valid_attr['teardown'])
        self.check_attr(name, 'doc', doc, self.valid_attr['doc'])
        self.check_attr(name, 'params', params, self.valid_attr['params'])
        self.check_attr(name, 'pos_arg', pos_arg, self.valid_attr['pos_arg'])
        self.check_attr(name, 'verbosity', verbosity, self.valid_attr['verbosity'])
        self.check_attr(name, 'io', io, self.valid_attr['io'])
        self.check_attr(name, 'getargs', getargs, self.valid_attr['getargs'])
        self.check_attr(name, 'title', title, self.valid_attr['title'])
        self.check_attr(name, 'watch', watch, self.valid_attr['watch'])
        self.check_attr(name, 'meta', meta, self.valid_attr['meta'])

        if '=' in name:
            msg = "Task '{}': name must not use the char '=' (equal sign)."
            raise InvalidTask(msg.format(name))
        self.name = name
        self.params = params  # save just for use on command `info`
        self.creator_params = []  # add through task_params decorator
        self.options = None
        self.pos_arg = pos_arg
        self.pos_arg_val = None  # to be set when parsing command line
        self.setup_tasks = list(setup)

        # actions
        self.io = IOConfig(io or {})
        self._action_instances = None
        if actions is None:
            self._actions = []
        else:
            self._actions = list(actions[:])

        self._init_deps(dependencies, calc_dep)

        # loaders create an implicit task_dep
        self.loader = loader
        if self.loader and self.loader.task_dep:
            self._dependencies.append(TaskDependency(loader.task_dep))

        uptodate = uptodate if uptodate else []

        self.getargs = getargs
        if self.getargs:
            uptodate.extend(self._init_getargs())

        self.value_savers = []
        self.uptodate = self._init_uptodate(uptodate)

        self.targets = self._init_targets(targets)
        self.subtask_of = subtask_of
        self.has_subtask = has_subtask
        self.result = None
        self.values = {}
        self.verbosity = verbosity
        self.custom_title = title
        self.cfg_values = None

        # clean
        if clean is True:
            self._remove_targets = True
            self.clean_actions = ()
        else:
            self._remove_targets = False
            self.clean_actions = [create_action(a, self, 'clean') for a in clean]

        self.teardown = [create_action(a, self, 'teardown') for a in teardown]
        self.doc = self._init_doc(doc)
        self.watch = watch
        self.meta = meta
        # just indicate if actions were executed at all
        self.executed = False


    def _init_deps(self, dependencies, calc_dep):
        """init for dependency related attributes"""
        self.dep_changed = None

        # Dependencies (Dependency objects)
        self._dependencies = []
        self.wild_dep = []  # Wildcard task deps like "task:*"
        self._init_dependencies(dependencies)

        # calc_dep
        self.calc_dep = set()
        if calc_dep:
            self._expand_calc_dep(calc_dep)

    def _init_dependencies(self, dependencies):
        """Initialize dependencies list from Dependency objects."""
        for dep in dependencies:
            if isinstance(dep, Dependency):
                # Handle wildcard task deps
                if isinstance(dep, TaskDependency) and "*" in dep.task_name:
                    self.wild_dep.append(dep.task_name)
                else:
                    self._dependencies.append(dep)
            else:
                msg = ("%s. dependencies must be Dependency objects. "
                       "Got '%r' (%s)")
                raise InvalidTask(msg % (self.name, dep, type(dep)))

    @property
    def dependencies(self):
        """Return list of Dependency objects."""
        return self._dependencies

    @property
    def file_dep(self):
        """Return set of file dependency paths (for backward compatibility).

        Returns the original paths (may be relative) to maintain compatibility
        with implicit task dependency resolution (target->file_dep matching).
        """
        return {d.path for d in self._dependencies
                if isinstance(d, FileDependency)}

    @property
    def task_dep(self):
        """Return list of task dependency names (for backward compatibility)."""
        return [d.task_name for d in self._dependencies
                if isinstance(d, TaskDependency)]

    def add_task_dep(self, task_name):
        """Add an implicit task dependency (e.g., from target->file_dep matching)."""
        if task_name not in self.task_dep:
            self._dependencies.append(TaskDependency(task_name))

    def add_file_dep(self, file_path):
        """Add a file dependency dynamically (e.g., from an action)."""
        if file_path not in self.file_dep:
            self._dependencies.append(FileDependency(file_path))

    def clear_task_deps(self):
        """Remove all task dependencies (used by --single mode)."""
        self._dependencies = [d for d in self._dependencies
                              if not isinstance(d, TaskDependency)]

    def clear_file_deps(self):
        """Remove all file dependencies (used for regex target matching)."""
        self._dependencies = [d for d in self._dependencies
                              if not isinstance(d, FileDependency)]

    def _init_targets(self, items):
        """convert valid targets to `str`"""
        targets = []
        for target in items:
            if isinstance(target, str):
                targets.append(target)
            elif isinstance(target, PurePath):
                targets.append(str(target))
            else:
                msg = ("%s. target must be a str or Path from pathlib. Got '%r' (%s)")
                raise InvalidTask(msg % (self.name, target, type(target)))
        return targets


    def _init_uptodate(self, items):
        """wrap uptodate callables"""
        uptodate = []
        for item in items:
            # configure task
            if hasattr(item, 'configure_task'):
                item.configure_task(self)

            # check/append uptodate value to task
            if isinstance(item, bool) or item is None:
                uptodate.append((item, None, None))
            elif hasattr(item, '__call__'):
                uptodate.append((item, [], {}))
            elif isinstance(item, tuple):
                call = item[0]
                args = list(item[1]) if len(item) > 1 else []
                kwargs = item[2] if len(item) > 2 else {}
                uptodate.append((call, args, kwargs))
            elif isinstance(item, str):
                uptodate.append((item, [], {}))
            else:
                msg = ("%s. task invalid 'uptodate' item '%r'. "
                       "Must be bool, None, str, callable or tuple "
                       "(callable, args, kwargs).")
                raise InvalidTask(msg % (self.name, item))
        return uptodate


    def _expand_calc_dep(self, calc_dep):
        """calc_dep input"""
        for dep in calc_dep:
            if dep not in self.calc_dep:
                self.calc_dep.add(dep)

    def _extend_uptodate(self, uptodate):
        """add/extend uptodate values"""
        self.uptodate.extend(self._init_uptodate(uptodate))

    def _extend_dependencies(self, dependencies):
        """add/extend dependencies"""
        for dep in dependencies:
            if isinstance(dep, Dependency):
                if isinstance(dep, TaskDependency) and "*" in dep.task_name:
                    self.wild_dep.append(dep.task_name)
                else:
                    self._dependencies.append(dep)
            else:
                msg = ("%s. dependencies must be Dependency objects. "
                       "Got '%r' (%s)")
                raise InvalidTask(msg % (self.name, dep, type(dep)))

    def _extend_file_dep(self, file_deps):
        """Add file dependencies from calc_dep results (legacy format).

        Converts string paths to FileDependency objects.
        """
        for dep in file_deps:
            if isinstance(dep, str):
                self._dependencies.append(FileDependency(dep))
            elif isinstance(dep, PurePath):
                self._dependencies.append(FileDependency(str(dep)))
            else:
                msg = ("%s. file_dep must be a str or Path. Got '%r' (%s)")
                raise InvalidTask(msg % (self.name, dep, type(dep)))

    def _extend_task_dep(self, task_deps):
        """Add task dependencies from calc_dep results (legacy format).

        Converts string task names to TaskDependency objects.
        """
        for dep in task_deps:
            if isinstance(dep, str):
                if "*" in dep:
                    self.wild_dep.append(dep)
                else:
                    self._dependencies.append(TaskDependency(dep))

    # FIXME should support setup also
    _expand_map = {
        'dependencies': _extend_dependencies,
        'calc_dep': _expand_calc_dep,
        'uptodate': _extend_uptodate,
        'file_dep': _extend_file_dep,
        'task_dep': _extend_task_dep,
    }

    def update_deps(self, deps):
        """expand all kinds of dep input"""
        for dep, dep_values in deps.items():
            if dep not in self._expand_map:
                continue
            self._expand_map[dep](self, dep_values)


    def init_options(self, args=None):
        """Put default values on options.

        This function will only initialize task options once. If provided the args
        parameter will be parsed for command line arguments intended for this task.

        Return value: unparsed command line task arguments or None.
        """
        if self.options is None:
            self.options = {}
            all_opts = list(self.params) + self.creator_params
            taskcmd = TaskParse([normalize_option(opt) for opt in all_opts])
            if self.cfg_values is not None:
                taskcmd.overwrite_defaults(self.cfg_values)

            if args is None or len(args) == 0:
                # ignore positional parameters
                self.options.update(taskcmd.parse('')[0])
            elif len(args) > 0:
                parsed_options, args = taskcmd.parse(args)
                self.options.update(parsed_options)

            return args

    def _init_getargs(self):
        """task getargs attribute define implicit task dependencies"""
        check_result = set()

        for arg_name, desc in self.getargs.items():

            # tuple (task_id, key_name)
            parts = desc
            if isinstance(parts, str) or len(parts) != 2:
                raise InvalidTask(
                    f"Taskid '{self.name}' - Invalid format for getargs of '{arg_name}'."
                    "Should be tuple with 2 elements"
                    f" ('<taskid>', '<key-name>') got '{desc}'")

            if parts[0] not in self.setup_tasks:
                check_result.add(parts[0])

        return [result_dep(t, setup_dep=True) for t in check_result]


    @staticmethod
    def _init_doc(doc):
        """process task "doc" attribute"""
        # store just first non-empty line as documentation string
        return first_line(doc)

    @staticmethod
    def check_attr(task, attr, value, valid):
        """check input task attribute is correct type/value

        @param task (string): task name
        @param attr (string): attribute name
        @param value: actual input from user
        @param valid (list): of valid types/value accepted
        @raises InvalidTask if invalid input
        """
        if isinstance(value, valid[0]):
            return
        if value in valid[1]:
            return

        # input value didnt match any valid type/value, raise exception
        msg = "Task '%s' attribute '%s' must be " % (task, attr)
        accept = ", ".join([getattr(v, '__name__', str(v)) for v in
                            (valid[0] + valid[1])])
        msg += "{%s} got:%r %s" % (accept, value, type(value))
        raise InvalidTask(msg)


    @property
    def actions(self):
        """lazy creation of action instances"""
        if self._action_instances is None:
            self._action_instances = [
                create_action(a, self, 'actions') for a in self._actions]
        return self._action_instances


    def save_extra_values(self):
        """run value_savers updating self.values"""
        for value_saver in self.value_savers:
            self.values.update(value_saver())

    def overwrite_verbosity(self, stream):
        self.verbosity = stream.effective_verbosity(self.verbosity)

    def execute(self, stream):
        """Executes the task.
        @return failure: see CmdAction.execute
        """
        self.executed = True
        self.init_options()
        task_stdout, task_stderr = stream._get_out_err(self.verbosity)
        for action in self.actions:
            action_return = action.execute(task_stdout, task_stderr)
            if isinstance(action_return, BaseFail):
                return action_return
            self.result = action.result
            self.values.update(action.values)


    def execute_teardown(self, stream):
        """Executes task's teardown
        @return failure: see CmdAction.execute
        """
        task_stdout, task_stderr = stream._get_out_err(self.verbosity)
        for action in self.teardown:
            action_return = action.execute(task_stdout, task_stderr)
            if isinstance(action_return, BaseFail):
                return action_return


    def clean(self, outstream, dryrun):
        """Execute task's clean
        @ivar outstream: 'write' output into this stream
        @ivar dryrun (bool): if True clean tasks are not executed
                             (just print out what would be executed)
        """
        self.init_options()
        # if clean is True remove all targets
        if self._remove_targets is True:
            clean_targets(self, dryrun)
        else:
            # clean contains a list of actions...
            for action in self.clean_actions:
                msg = "%s - executing '%s'\n"
                outstream.write(msg % (self.name, action))

                # add extra arguments used by clean actions
                execute_on_dryrun = False
                if isinstance(action, PythonAction):
                    action_sig = inspect.signature(action.py_callable)
                    if 'dryrun' in action_sig.parameters:
                        execute_on_dryrun = True
                        action.kwargs['dryrun'] = dryrun

                if (not dryrun) or execute_on_dryrun:
                    result = action.execute(out=outstream)
                    if isinstance(result, BaseFail):
                        sys.stderr.write(str(result))

    def title(self):
        """String representation on output.

        @return: (str) Task name and actions
        """
        if self.custom_title:
            return self.custom_title(self)
        return self.name


    def __repr__(self):
        return f"<Task: {self.name}>"

    def __eq__(self, other):
        return self.name == other.name

    def __lt__(self, other):
        """used on default sorting of tasks (alphabetically by name)"""
        return self.name < other.name



def dict_to_task(task_dict):
    """Create a task instance from dictionary.

    The dictionary has the same format as returned by task-generators
    from dodo files.

    @param task_dict (dict): task representation as a dict.
    @raise InvalidTask: If unexpected fields were passed in task_dict
    """
    # check required fields
    if 'actions' not in task_dict:
        raise InvalidTask("Task %s must contain 'actions' field. %s" %
                          (task_dict['name'], task_dict))

    # user friendly. dont go ahead with invalid input.
    task_attrs = list(task_dict.keys())
    valid_attrs = set(Task.valid_attr.keys())
    for key in task_attrs:
        if key not in valid_attrs:
            name = task_dict['name']
            raise InvalidTask(f"Task {name} contains invalid field: '{key}'")

    # Convert legacy file_dep/task_dep to dependencies
    task_dict = _convert_legacy_deps(task_dict)

    return Task(**task_dict)


def _convert_legacy_deps(task_dict):
    """Convert legacy file_dep/task_dep dict keys to dependencies.

    This maintains backward compatibility with dodo file format.
    """
    file_dep = task_dict.pop('file_dep', None)
    task_dep = task_dict.pop('task_dep', None)

    if file_dep is None and task_dep is None:
        return task_dict

    # Get existing dependencies or create empty list
    deps = list(task_dict.get('dependencies', []))

    # Convert file_dep strings to FileDependency objects
    if file_dep:
        for dep in file_dep:
            if isinstance(dep, str):
                deps.append(FileDependency(dep))
            elif isinstance(dep, PurePath):
                deps.append(FileDependency(str(dep)))
            else:
                name = task_dict.get('name', '<unknown>')
                msg = "%s. file_dep must be a str or Path. Got '%r' (%s)"
                raise InvalidTask(msg % (name, dep, type(dep)))

    # Convert task_dep strings to TaskDependency objects
    if task_dep:
        for dep in task_dep:
            if isinstance(dep, str):
                deps.append(TaskDependency(dep))
            else:
                name = task_dict.get('name', '<unknown>')
                msg = "%s. task_dep must be a str. Got '%r' (%s)"
                raise InvalidTask(msg % (name, dep, type(dep)))

    task_dict['dependencies'] = deps
    return task_dict



def clean_targets(task, dryrun):
    """remove all targets from a task"""
    for target in sorted(task.targets, reverse=True):
        if os.path.isfile(target):
            print("%s - removing file '%s'" % (task.name, target))
            if not dryrun:
                os.remove(target)
        elif os.path.isdir(target):
            if os.listdir(target):
                msg = "%s - cannot remove (it is not empty) '%s'"
                print(msg % (task.name, target))
            else:
                msg = "%s - removing dir '%s'"
                print(msg % (task.name, target))
                if not dryrun:
                    os.rmdir(target)


# uptodate
class result_dep(UptodateCalculator):
    """check if result of the given task was modified
    """
    def __init__(self, dep_task_name, setup_dep=False):
        '''
        :param setup_dep: controls if dependent task is task_dep or setup
        '''
        self.dep_name = dep_task_name
        self.setup_dep = setup_dep
        self.result_name = '_result:%s' % self.dep_name

    def configure_task(self, task):
        """to be called by doit when create the task"""
        # result_dep creates an implicit task_dep
        if self.setup_dep:
            task.setup_tasks.append(self.dep_name)
        else:
            task._dependencies.append(TaskDependency(self.dep_name))


    def _result_single(self):
        """get result from a single task"""
        return self.get_val(self.dep_name, StorageKey.RESULT)

    def _result_group(self, dep_task):
        """get result from a group task
        the result is the combination of results of all sub-tasks
        """
        prefix = dep_task.name + ":"
        sub_tasks = {}
        for sub in dep_task.task_dep:
            if sub.startswith(prefix):
                sub_tasks[sub] = self.get_val(sub, StorageKey.RESULT)
        return sub_tasks

    def _get_dep_result(self, dep_task):
        if not dep_task.has_subtask:
            dep_result = self._result_single()
        else:
            dep_result = self._result_group(dep_task)
        return dep_result


    def __call__(self, task, values):
        """return True if result is the same as last run"""
        dep_task = self.tasks_dict[self.dep_name]
        dep_result = self._get_dep_result(dep_task)

        def result_saver():
            # get latest value after execution of dependent task
            return {self.result_name: self._get_dep_result(dep_task)}
        task.value_savers.append(result_saver)

        last_success = values.get(self.result_name)
        if last_success is None:
            return False
        return last_success == dep_result
