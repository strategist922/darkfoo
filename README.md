#DarkFoo - A Pig UDF library
-----------------------------------------------------------------------------------------------------
DarkFoo is a library of pig UDFs written in Java. It is available through GPLv.3 and at the time of writing contains the following functions:
-Delete URLs from tuple.
-Exchange all the tabs for spaces in a tuple.
-Change all consecutive spaces to single spaces
-Change all consecutive dots to single dots.
-Make Markov chain style pairs from a bag of words.
-Remove all chars from tuple except for ASCII chars, nums and spaces.
-Take a tuple and split it at space and return the results in a bag.
-Take a tuple and split it at dot and return the results in a bag.
-Remove twitter style "@name" from tuple.
-Remove twitter style hash-tags from tuple.


##Download
-----------------------------------------------------------------------------------------------------
To download the source simply clone the git repo.
###Clone
>git clone git@git.life-hack.org:lazy-drone/pigudf-darkfoo.git

##Build
-----------------------------------------------------------------------------------------------------
To build and install ant is required. Also Hadoop-common and pig jars are required. Before starting installation make sure to change the paths to the jars in build.xml

###Regualr build
>ant build

###To rebuild clean
>ant clean-build

###To clean up class files and leftover jars
>ant clean