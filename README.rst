=======
tnt-pyq
=======

Python Bindings for `Tarantool Queue <https://github.com/tarantool/queue/>`_.

Library depends on:

 * msgpack-python 
 * tarantool

Basic usage can be found in tests. Description on every command is in source code.

Big thanks to Dmitriy Shveenkov and Alexandr (FZambia) Emelin.

For install it type:

.. code-block:: bash

    $ sudo pip install -e git+https://github.com/bigbes92/tnt-pyq.git

For configuring Queue in `Tarantool <http://tarantool.org>`_ read manual `Here <https://github.com/tarantool/queue>`_.

Then just **import** it, create **Queue**, create **Tube**, **put** and **take** some elements:
    
.. code-block:: python

    >>> from tntqueue import Queue    
    >>> queue = Queue("localhost", 33013, 0)
    >>> tube = queue.create_tube("name_of_tube")
    >>> tube.put([1, 2, 3])
    Not taken task instance
    >>> task = tube.take()
    >>> task.data # take task and read data from it
    [1, 2, 3]
    >>> task.ack() # move this task into state DONE
    True

That's all, folks!
