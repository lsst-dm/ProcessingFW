# $Id:$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit. 
# $LastChangedDate::                      $:  # Date of last commit.

SHELL=/bin/sh

build:
	@echo "ProcessingFW: Ready to install"

install: 
ifndef INSTALL_ROOT
	@echo "ProcessingFW: Must define INSTALL_ROOT"
	false
endif
	@echo "ProcessingFW: Installing to ${INSTALL_ROOT}"
	-mkdir -p ${INSTALL_ROOT}
	-rsync -Caq bin ${INSTALL_ROOT}
#	-rsync -Caq etc ${INSTALL_ROOT}
	-mkdir -p ${INSTALL_ROOT}/python
	-rsync -Caq python/processingfw ${INSTALL_ROOT}/python
	-rsync -Caq libexec ${INSTALL_ROOT}
	-rsync -Caq share ${INSTALL_ROOT}
#	-rsync -Caq man ${INSTALL_ROOT}
	@echo "Make sure ${INSTALL_ROOT}/python is in PYTHONPATH"

test:
	@echo "ProcessingFW: tests are currently not available"

clean:
	rm -f python/processingfw/*.pyc
