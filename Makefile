BINDIR=bin
BINARIES = $(addprefix $(BINDIR)/, mucor3)
LIBRARY_PATH := ${LD_LIBRARY_PATH}
LD_LIBRARY_PATH := ${LD_LIBRARY_PATH}
LINKERVARS := LD_LIBRARY_PATH=$(LD_LIBRARY_PATH) LIBRARY_PATH=$(LIBRARY_PATH) 
D := $(LINKERVARS) dub build  
RELEASE := --build release

ifdef USEDMD
	DC := --compiler dmd
else
	DC := --compiler ldc2
endif

ifdef DEBUG
	DUB :=$(D)
else
	DUB :=$(D) $(RELEASE)
endif



all: $(BINARIES)

$(BINARIES): $(shell find . -type f -name "*.d")
ifdef STATIC
	cd $(notdir $@); $(DUB) $(DC) -c static-alpine $(notdir $@)
else
	cd $(notdir $@); $(DUB) $(DC) $(notdir $@)
endif

clean:
	rm bin/*

# mucor3:
# 	cd mucor3
# 	python setup.py install
