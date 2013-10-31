.. _quick_start_en:

===========
Quick Start
===========

--------------
Prepare server
--------------

1. Install tarantool on your favourite OS. For more info please read `User Guide <http://tarantool.org/tarantool_user_guide.html>`_ (Section Downloading and installing a binary package).
2. Download tarantool.cfg and init.lua from `Queue <https://github.com/tarantool/queue>`_ repo

.. code-block:: bash

	$ wget https://raw.github.com/tarantool/queue/master/tarantool.cfg # Download configuration file for tarantool
	$ wget https://github.com/tarantool/queue/blob/master/init.lua # Download queue script
	$ tarantool_box --init-storage # Init tarantool storage files
	$ tarantool_box # Start tarantool

Done! 

------------------------------
Install tarantool-queue-python
------------------------------

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

-----------------------------------------
Connecting to server and basic operations
-----------------------------------------

In the beggining you must import tarantool-queue and create queue object:

.. code-block:: python

	from tarantool-queue import Queue
	queue = Queue('localhost', 33020, 0)
	
Queue object is an aggregator for Tubes: Tube is the queue. When you put task into queue, you associate name of tube with it.
For the beggining you must create Tube object with *Queue.tube(name)* method.
Basic operations for Tubes are: *Tube.put(task)*  and *Tube.get()* 
When you have done all you want with this task you must make *Task.ack()* it or *Task.release()* it back to the queue.

.. code-block:: python
	
	# On producer:
	appetizers = queue.tube('appt-s')
	appetizers.put('Egg-Bacon') # put meal 
	appetizers.put('Egg-Sausage-Bacon')
	appetizers.put('Egg and Spam')
	appetizers.put('Egg-Bacon and Spam')
	appetizers.put('Egg-Bacon-Sausage')
	appetizers.put('Spam-Bacon-Sausage-Spam')
	appetizers.put('Spam-Egg-Spam-Spam-Bacon-Spam')
	appetizers.put('Spam-Spam-Spam-Egg-Spam')
	# Spam, Spam, Spam, Spam… Lovely Spam! Wonderful Spam!
	...

.. code-block:: python
	
	# On consumer number 1 (Viking):
	appetizers = queue.tube('appt-s')
	meal = appetizers.take(30) # wait for 'meal' in blocking mode for 30 seconds
	while meal is not None:
		if meal.data.find('Spam') == -1: # we don't want meal without spam
			meal.release(delay=1)
		else:
			eat(meal.data) # do something
			meal.ack() # acknowlege, that you did all you want with this 'meal'
		meal = appetizers.take(30) # take next meal
	exit(1) # no meal for 30 seconds, go out from here
	
.. code-block:: python
	
	# On consumer number 2 (Missus):
	appetizers = queue.tube('appt-s')
	meal = appetizers.take(30) # wait for 'meal' in blocking mode for 30 seconds
	while meal is not None:
		if meal.data.find('Spam') != -1: # she is tired from spam
			meal.release(delay=1) 
		else:
			eat(meal.data) # do something
			meal.ack() # acknowlege, that you did all you want with this 'meal'
		meal = appetizers.take(30) # take next meal
	exit(1) # no meal for 30 seconds, go out from here

^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
What if we forget to ack or release the task?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Task class has destructor, that automaticly releases the task, if you have done nothing with it. e.g:
	
.. code-block:: python
	
	# You're consumer of some great spam:
	def eat_spam(tube):
		meal = tube.take()
		if (meal.data.find('Spam') != -1) 
			meal.ack()
			consume(meal) # do_something
		return # oops! we forget to release task if it has not spam in it!
		# but that's ok, GC will do it when his time will come. 
		
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
What data we can push into tubes?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Queue uses `msgpack <http://msgpack.org/>`_ (It's like JSON. but fast and small) for default `serializing` of data, so by default you may `serialize` *dicts, tuples/lists, strings, numbers and others basic types*.

If you want to push another objects to Tubes you may define another `serializers`. By default `serializers` of Tubes are None and it uses Queue `serializer`. If you set Tube `serializer` to callable object it will use it, instead of Queue `serializer`. e.g.:
	
.. code-block:: python
	
	import bz2
	import json
	import pickle
	
	from tarantool-queue import Queue
	
	queue = Queue('localhost', 33020, 0)
	
	jsons = queue.tube('json')
	jsons.serialize =   (lambda x: json.dumps(x)) # it's not necessary to use lambda in your projects
	jsons.deserialize = (lambda x: json.loads(x)) # but object, that'll serialize and deserialize must be callable or None
	
	pickls = queue.tube('pickle')
	pickls.serialize =   (lambda x: pickle.dump(x))
	pickls.deserialize = (lambda x: pickle.load(x))
	
	bz2s = queue.tube('bz2')
	bz2s.serialize =   (lambda x: bz2.compress(json.dumps(x)))
	bz2s.deserialize = (lambda x: json.loads(bz2.decompress(x)))
	
	default = queue.tube('default')	
	
	jsons.put([1, 2, 3])  # it will put [1, 2, 3] in json into queue.
	pickls.put([2, 3, 4]) # it will pickle [2, 3, 4] and put it into queue.
	bz2.put([3, 4, 5])    # it will bzip' [3, 4, 5] in json and put it into queue.
	
	default.put([4, 5, 6]) # msgpack will pack it and put into queue.
	queue.serialize =   (lambda x: pickle.dump(x))
	queue.deserialize = (lambda x: pickle.load(x))
	default.put([4, 5, 6]) # but now it'll be pickled.
	
	# to reset serializers you must simple assign None to serializer:
	queue.serialize =   None # it will restore msgpack as serializer
	queue.deserialize = None # it will restore msgpack as deserializer
	bz2s.serialize =   None # it will tell python to use Queue serializer instead of bz2
	bz2s.deserialize = None # it will tell python to use Queue deserializer instead of bz2

^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
But, i have very important task that needs to be done!
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

It's OK! You must use *Tube.urgent(data)*!

.. code-block:: python
	
	appetizers = queue.tube('appt-s')
	appetizers.put('Egg-Bacon') # put meal 
	appetizers.put('Egg-Sausage-Bacon') # another boring meal
	appetizers.urgent('Spam-Egg-Spam-Spam-Bacon-Spam') # very very tasty meal with a lot of SPAM
	
	meal1 = appetizers.take() ; print meal1.data # Spam-Egg-Spam-Spam-Bacon-Spam
	meal2 = appetizers.take() ; print meal2.data # Egg-Bacon
	meal3 = appetizers.take() ; print meal3.data # Egg-Sausage-Bacon
	
	meal1.ack() ; meal2.ack() ; meal3.ack()
	
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Ok! But i've some spam today! I want to know how much.
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

	appetizers = queue.tube('appt-s')
	appetizers.statistics() # will show you how many spam you've 'baked' and 'sold'
	queue.statistics() # will show you overall stats of your cafe
	
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
I have some spam, that is so awfully bad, that i want to bury deep inside.
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

	appetizers = queue.tube('appt-s')
	task = appetizers.get()
	task.bury() # it will bury meal deep inside
	task.dig() # it will 'unbury' meal, if you'll need it in future.
	task.delete() # it will destroy your 'meal' once and for all.
	appetizers.kick(number) # it will 'unbury' a number	of tasks in this Tube.
	task.done('New great SPAM with SPAM and HAM') # or you may replace this 'meal' with another.
	
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
But *Task.release()* returns task into the beggining! I want it to be in the end!
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Simply use *Task.requeue()* instead of *Task.release()*!

^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
SUDDENLY I have UUID of my 'meal', and i REALLY REALLY want this meal. What should i do?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You must use Queue.peek(uuid) method!

.. code-block:: python

	appetizers = queue.tube('appt-s')
	meal_uuid = '550e8400-e29b-41d4-a716-446655440000'
	task = queue.peek(meal_uuid)
	print task.data # Spam-Egg-Spam-Spam-Bacon-Spam

^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
And Now for Something Completely Different..
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Thank's to Monty Python's comic troop!

.. image:: http://www.madmumblings.com/gallery/albums/userpics/10026/spam%20spam%20lovely%20spam.jpg
