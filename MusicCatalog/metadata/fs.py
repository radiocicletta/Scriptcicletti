import sys
if sys.platform.startswith('linux'):
    from metadata.listeners.linux import InotifySubtreeListener as SubtreeListener, InotifyThread as FSThread
if sys.platform.startswith('darwin'):
    from metadata.listeners.osx import FseventsSubtreeListener as SubtreeListener, FseventsThread as FSThread


def create_subtreelistener(path, dbpath, queues, condition):
    """ create a subtree listener """
    listener = SubtreeListener(dbpath, queues, condition).get_handler()
    thread = FSThread(listener)
    thread.start()
    thread.observe(path, excludelist=["(.*)sqlite"])
    return thread
