#!/bin/bash

function log() {
  echo -e $(date +"%F %T") -- $@ >&2
}

set -o errexit
set -o nounset

log "Installing stuff"

CodePath="${HOME}/code"
InstallPath="${HOME}/usr"
PythonInstallPath="${InstallPath}/lib/python2.7/site-packages/"
export PYTHONPATH="${PythonInstallPath}:${PYTHONPATH}"

cd "${CodePath}"

# dependencies
log "Installing packaged dependencies with apt-get"
#sudo apt-get update
sudo apt-get install libpython2.7-dev python-pytest swig3.0


# install SBT
if ! type -P sbt ; then
	log "sbt executable not found.  Downloading and installing it"
	wget -O - https://dl.bintray.com/sbt/native-packages/sbt/0.13.12/sbt-0.13.12.tgz | tar xzf -
	mkdir -p "${HOME}/bin"
	ln -sf "${CodePath}/sbt/bin/sbt" "${HOME}/bin/sbt"
	log "linked sbt executable to ${HOME}/bin/sbt"
	if ! type -P sbt ; then
		log "It seems that ${HOME}/bin isn't in your PATH.  Going to add it at the bottom of .bashrc"
		# not in PATH
		export PATH="${HOME}/bin:${PATH}"
		echo 'export PATH="${HOME}/bin:${PATH}"' >> ~/.bashrc
	fi
fi


# install our programs

# Python setuptools won't create the installation directory if it doesn't exist
mkdir -p "${PythonInstallPath}"


log "Installing Pyavroc to ${InstallPath}"
log "Removing ${CodePath}/pyavroc, if it exists"
rm -rf "${CodePath}/pyavroc"
git clone https://github.com/Byhiras/pyavroc.git
cd pyavroc
./clone_avro_and_build.sh
python setup.py install --prefix "${InstallPath}"


cd "${CodePath}"


log "Installing Pydoop to ${InstallPath}"
log "Removing ${CodePath}/pydoop, if it exists"
rm -rf "${CodePath}/pydoop"
git clone https://github.com/crs4/pydoop.git
cd pydoop
python setup.py build
python setup.py install --prefix "${InstallPath}"

cd "${CodePath}"

log "Installing RAPI to ${InstallPath}"
git clone https://github.com/crs4/rapi.git
cd rapi
make
cd pyrapi
python setup.py install --prefix "${InstallPath}"

cd "${CodePath}"

log "Installing Seal to ${InstallPath}"
log "Removing ${CodePath}/seal, if it exists"
rm -rf "${CodePath}/seal"
git clone https://github.com/crs4/seal.git
cd seal
git checkout develop
python setup.py build
python setup.py install --prefix "${InstallPath}"

if ! type -P pydoop; then
	log  "Adding ${InstallPath}/bin to PATH defined in .bashrc"
	echo "export PATH=\"${InstallPath}/bin:\${PATH}\"" >> ~/.bashrc
	log "Remember to reload your PATH"
fi

if ! grep "PYTHONPATH.*${InstallPath}" ~/.bashrc ; then
	log "Adding ${InstallPath} to PYTHONPATH"
	echo "export PYTHONPATH=\"${InstallPath}/lib/python2.7/site-packages/:\${PYTHONPATH}\"" >> ~/.bashrc
fi

log "All done"
