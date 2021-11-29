LIBRARY_PATH := /usr/local/lib
LD_LIBRARY_PATH := LIBRARY_PATH:${LD_LIBRARY_PATH}
LINKERVARS := LD_LIBRARY_PATH=$(LD_LIBRARY_PATH) LIBRARY_PATH=$(LIBRARY_PATH) 
D := $(LINKERVARS) dub build --build release

all: all_d mucor3
all_d: atomization/atomize_vcf/atomize_vcf depthGauge/depthgauge filtering/varquery/varquery manipulation/wrangler/wrangler

clean:
	rm atomization/atomize_vcf/atomize_vcf
	rm depthGauge/depthgauge
	rm filtering/varquery/varquery
	rm manipulation/wrangler/wrangler

atomization/atomize_vcf/atomize_vcf:
	cd atomization/atomize_vcf;$(D)
	

depthGauge/depthgauge:
	cd depthGauge;$(D)

filtering/varquery/varquery:
	cd filtering/varquery;$(D)

manipulation/wrangler/wrangler:
	cd manipulation/wrangler;$(D)

mucor3:
	cd mucor3
	python setup.py install
