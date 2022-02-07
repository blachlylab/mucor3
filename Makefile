BINDIR=bin
BINARIES = $(addprefix $(BINDIR)/, atomize_vcf depthgauge varquery wrangler)
LIBRARY_PATH := /usr/local/lib
LD_LIBRARY_PATH := LIBRARY_PATH:${LD_LIBRARY_PATH}
LINKERVARS := LD_LIBRARY_PATH=$(LD_LIBRARY_PATH) LIBRARY_PATH=$(LIBRARY_PATH) 
D := $(LINKERVARS) dub build --build release 

all: $(BINARIES) mucor3

$(BINARIES):
ifdef STATIC
	$(D) -c static-alpine mucor3:$(notdir $@)
else 
	$(D) mucor3:$(notdir $@)
endif

clean:
	rm bin/*

mucor3:
	cd mucor3
	python setup.py install
