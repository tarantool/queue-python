.. _codeapi:

Queue API
*********

Basic Definitions:

* ttl - Time to Live of task.
* ttr - Time to Release of task.
* pri - Priority of task.
* delay - Delay for task to be added to queue.

.. automodule:: tarantool_queue

.. warning:: 
    
    Don't use constructor of Task and Tube.
    Task's are created by Tube and Queue methods.
    For creating Tube object use :meth:`Queue.tube(name) <tarantool_queue.Queue.tube>`

.. autoclass:: Queue
    :members:

.. autoclass:: Tube
    :members:

.. autoclass:: Task
    :members:
