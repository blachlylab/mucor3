all: atomization/atomize_vcf/atomize_vcf depthGauge/depthgauge filtering/varquery/varquery mucor3

clean:
	rm atomization/atomize_vcf/atomize_vcf
	rm depthGauge/depthgauge
	rm filtering/varquery/varquery

atomization/atomize_vcf/atomize_vcf:
	cd atomization/atomize_vcf;dub build --build release
	

depthGauge/depthgauge:
	cd depthGauge;dub build --build release

filtering/varquery/varquery:
	cd filtering/varquery;dub build --build release

mucor3:
	cd mucor3
	python setup.py install