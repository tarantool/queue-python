======================
tarantool-queue-python
======================

Python Bindings for `Tarantool Queue <https://github.com/tarantool/queue/>`_.

Library depends on:

* msgpack-python 
* tarantool

Basic usage can be found in tests. Description on every command is in source code.

Big thanks to Dmitriy Shveenkov and `Alexandr (FZambia) Emelin <https://github.com/FZambia>`_.

For install of latest "stable" version type:

.. code-block:: bash

    # using pip
    $ sudo pip install tarantool-queue
    # or using easy_install
    $ sudo easy_install tarantool-queue
    # or using python
    $ wget http://is.gd/tarantool_queue
    $ tar xzf tarantool-queue-0.1.0.tar.gz
    $ cd tarantool-queue-0.1.0
    $ sudo python setup.py install

For install bleeding edge type:

.. code-block:: bash

    $ sudo pip install git+https://github.com/bigbes92/tarantool-queue-python.git

For configuring Queue in `Tarantool <http://tarantool.org>`_ read manual `Here <https://github.com/tarantool/queue>`_.

Then just **import** it, create **Queue**, create **Tube**, **put** and **take** some elements:
    
.. code-block:: python

    >>> from tarantool_queue import Queue 
    >>> queue = Queue("localhost", 33013, 0)
    >>> tube = queue.tube("name_of_tube")
    >>> tube.put([1, 2, 3])
    Not taken task instance
    >>> task = tube.take()
    >>> task.data # take task and read data from it
    [1, 2, 3]
    >>> task.ack() # move this task into state DONE
    True

That's all, folks!

See Also
========
* `Documentation <http://tarantool-queue-python.readthedocs.org/en/latest/>`_
* :ref:`quick_start_en`
* :ref:`codeapi`
