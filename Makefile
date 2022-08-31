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

$(BINARIES): $(shell find . -type f -name "*.d") mucor3/dub.json
ifdef STATIC
	cd $(notdir $@); $(DUB) $(DC) -c static-alpine $(notdir $@)
else
	cd $(notdir $@); $(DUB) $(DC) $(notdir $@)
endif

clean:
	# remove binaries
	[ ! -e bin ] || rm -r bin

	# remove dub selection files
	[ ! -e libmucor/dub.selections.json ] || rm libmucor/dub.selections.json
	[ ! -e mucor3/dub.selections.json ] || rm mucor3/dub.selections.json

	# remove .dub folders
	[ ! -e libmucor/.dub ] || rm -r libmucor/.dub
	[ ! -e mucor3/.dub ] || rm -r mucor3/.dub

# mucor3:
# 	cd mucor3
# 	python setup.py install
