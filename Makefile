BINDIR=bin
BINARIES = $(addprefix $(BINDIR)/, mucor3)
LIBRARY_PATH := ${LD_LIBRARY_PATH}
LD_LIBRARY_PATH := ${LD_LIBRARY_PATH}
LINKERVARS := LD_LIBRARY_PATH=$(LD_LIBRARY_PATH) LIBRARY_PATH=$(LIBRARY_PATH) 
D := $(LINKERVARS) dub build --build release 

all: $(BINARIES)

$(BINARIES): $(shell find . -type f -name "*.d")
ifdef STATIC
	cd $(notdir $@); $(D) -c static-alpine $(notdir $@)
else 
	cd $(notdir $@); $(D) $(notdir $@)
endif

clean:
	rm bin/*

# mucor3:
# 	cd mucor3
# 	python setup.py install
