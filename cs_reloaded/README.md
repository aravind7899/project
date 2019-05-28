This is a revised module created to perform operations on cassandra.Read install.txt(link is https://github.com/aravind7899/cs_reloaded/blob/master/install.txt ) to install this module.This should be used after the installation of cassandra.Here is the link for the installation of cassandra(http://cassandra.apache.org/download/ ).The prerequisits for the installation of cassandra are latest version of Java8 and in order to use cqlsh(cassandra quer language shell), python of version 2.7 is used.In order to use this module,the modules that are to be pre installed is:
1.Cassandra-driver
Installation of cassandra driver:
Installation through pip
pip is the suggested tool for installing packages. It will handle installing all Python dependencies for the driver at the same time as the driver itself. To install the driver*:

pip install cassandra-driver

You can use pip install --pre cassandra-driver if you need to install a beta version.

OSX Installation Error
If you’re installing on OSX and have XCode 5.1 installed, you may see an error like this:

clang: error: unknown argument: '-mno-fused-madd' [-Wunused-command-line-argument-hard-error-in-future]
To fix this, re-run the installation with an extra compilation flag:

ARCHFLAGS=-Wno-error=unused-command-line-argument-hard-error-in-future pip install cassandra-driver

Manual Installation
You can always install the driver directly from a source checkout or tarball. When installing manually, ensure the python dependencies are already installed. You can find the list of dependencies.

Six - To install this use command :pip install six>=1.9

futures - To install this use command :pip install futures<=2.2.0

Once the dependencies are installed, simply run:

python setup.py install

