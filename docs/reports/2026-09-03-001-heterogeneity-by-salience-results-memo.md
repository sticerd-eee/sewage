# Heterogeneity by Local Salience: results memo

Snapshot: 3 September 2026. Sales 2021–2024; rentals 2021–2023. Source analysis commit: `c58fa4cdccaf5efa3f5d0d107713b9d1219643e2` on `jo/site-heterogeneity`.

Historical snapshot: the four-way exploration below now lives in the
[executable salience report](2026-09-03-003-heterogeneity-by-salience-report.qmd).
The five paper scripts now estimate overlapping All Bathing, All Coastal and
All Inland groups and publish separate `_groups` outputs.

## Results at the 2 km coast rule

- **Extensive Margin.** The sales Near × Post coefficient is -0.01200 (0.00991) for coastal bathing and -0.01245 (0.00532) for coastal not bathing. Both inland sales estimates are close to zero. For coastal not bathing rentals it is -0.00829 (0.00459). Parentheses contain standard errors.
- **Intensive Margin.** The sales spill-count × Post estimates range from −0.00270 to 0.01537, with standard errors from 0.00564 to 0.02469. For inland not bathing rentals, the Post interaction is 0.00780 (0.00335) and the articles interaction is 0.00299 (0.00104).
- **Baseline hedonic.** The weekly spill-count coefficient is -0.07047 (0.02928) for inland bathing sales and -0.02472 (0.00228) for inland not bathing rentals. The inland bathing rental samples contain only 267 observations in the Intensive Margin attention models and 255 in the hedonic.

Across the robustness variants, the Extensive Margin coastal not bathing sales Post estimate is -0.01534 (0.00390) under the 10 km coast rule, 0.00077 (0.00607) with London retained, and -0.01275 (0.00556) after dropping unresolved bathing evidence. Retaining London changes the sign of this sales estimate.

These are separate within-stratum estimates. Differences between them have not been tested with a pooled cross-stratum interaction test.

## Reading the estimates and counts

Local Salience defines each cross-sectional Salience Stratum. Public Attention is either Post (August 2022 onward, including August) or log cumulative UK Media Article Count (Articles). The reported attention coefficient is Near × Attention for the Extensive Margin and average weekly spill count × Attention for the Intensive Margin. The baseline hedonic reports the average weekly spill-count coefficient. Outcomes are log sale price or log weekly asking rent; coefficients below are in log units, not percentages.

Extensive Margin compares properties 0–500 m from an overflow (inclusive) with those >1,000 m and ≤2,000 m away. Intensive Margin and baseline hedonic use properties within 250 m. All models retain the parent property controls and LSOA fixed effects. Attention models include month fixed effects and LSOA-clustered SEs; the hedonic has no time fixed effects and uses heteroskedasticity-robust SEs.

Coast and bathing are independent: coastal means Site Coast Distance ≤2 km; bathing means an Ever-Observed Designated-Water Indicator in 2021–2024. Extensive Margin and hedonic use the nearest Site Group; Intensive Margin uses any bathing Site Group and minimum coast distance in the 250 m companion. A positive designation takes precedence over other unknown evidence. Unresolved designation counts as not bathing in the headline.

Greater London is excluded unless labelled **London retained**. Site Coast Distance follows the tidal Mean High Water line, which classifies locations along the tidal Thames as coast. This exclusion does not turn the measure into distance to the open sea.

**Variants:** **2 km** is the headline coast/bathing family; **10 km** changes only the coast threshold; **London retained** changes only the London exclusion; **Drop unresolved** excludes unknown or missing bathing evidence without a positive. **Intensity** uses the Property Spill-Intensity Band at 500 m (Extensive Margin near group) or 250 m (Intensive Margin), excluding zero and unknown exposure. The full far group appears in both extensive intensity samples, so those rows must not be added together. Published band cutoffs are not recomputed after dropping London. The hedonic has no intensity split.

**Counts:** Before and After count the stratum before and after the London drop, before estimator removals. N is the final regression sample. For London retained, estimation starts from Before; otherwise it starts from After. Post and Articles have identical counts in every corresponding row, verified from both model bundles and audit CSVs. Coefficients and SEs are shown to five decimal places from the saved models; the LaTeX tables round to three.

## Extensive Margin

### Sales

|Variant         |Stratum             |    Before|     After|         N| Post interaction (SE)| Articles interaction (SE)|
|:---------------|:-------------------|---------:|---------:|---------:|---------------------:|-------------------------:|
|2 km            |Coastal bathing     |   163,456|   163,456|   163,420|    -0.01200 (0.00991)|        -0.00261 (0.00221)|
|2 km            |Coastal not bathing |   324,729|   241,858|   241,780|    -0.01245 (0.00532)|        -0.00265 (0.00166)|
|2 km            |Inland bathing      |    30,438|    30,438|    30,410|     0.00019 (0.01344)|         0.00085 (0.00435)|
|2 km            |Inland not bathing  | 1,439,067| 1,321,110| 1,320,899|     0.00203 (0.00248)|         0.00029 (0.00076)|
|Intensity       |Positive ≤ median   | 1,418,529| 1,258,323| 1,257,847|    -0.00201 (0.00342)|         0.00033 (0.00127)|
|Intensity       |Positive > median   | 1,418,525| 1,261,557| 1,261,023|     0.00630 (0.00366)|         0.00316 (0.00120)|
|10 km           |Coastal bathing     |   184,404|   184,404|   184,360|    -0.00960 (0.00908)|        -0.00198 (0.00208)|
|10 km           |Coastal not bathing |   694,398|   527,762|   527,662|    -0.01534 (0.00390)|        -0.00493 (0.00119)|
|10 km           |Inland bathing      |     9,490|     9,490|     9,483|    -0.00890 (0.01842)|         0.00190 (0.00613)|
|10 km           |Inland not bathing  | 1,069,398| 1,035,206| 1,035,043|     0.00677 (0.00276)|         0.00207 (0.00085)|
|London retained |Coastal bathing     |   163,456|   163,456|   163,420|    -0.01200 (0.00991)|        -0.00261 (0.00221)|
|London retained |Coastal not bathing |   324,729|   241,858|   324,633|     0.00077 (0.00607)|         0.00110 (0.00171)|
|London retained |Inland bathing      |    30,438|    30,438|    30,410|     0.00019 (0.01344)|         0.00085 (0.00435)|
|London retained |Inland not bathing  | 1,439,067| 1,321,110| 1,438,838|     0.00313 (0.00242)|         0.00089 (0.00074)|
|Drop unresolved |Coastal bathing     |   163,456|   163,456|   163,420|    -0.01200 (0.00991)|        -0.00261 (0.00221)|
|Drop unresolved |Coastal not bathing |   291,402|   227,842|   227,759|    -0.01275 (0.00556)|        -0.00249 (0.00173)|
|Drop unresolved |Inland bathing      |    30,438|    30,438|    30,410|     0.00019 (0.01344)|         0.00085 (0.00435)|
|Drop unresolved |Inland not bathing  | 1,244,062| 1,209,327| 1,209,100|     0.00074 (0.00260)|         0.00003 (0.00080)|

### Rentals

|Variant         |Stratum             |  Before|   After|       N| Post interaction (SE)| Articles interaction (SE)|
|:---------------|:-------------------|-------:|-------:|-------:|---------------------:|-------------------------:|
|2 km            |Coastal bathing     |  37,904|  37,904|  37,715|    -0.00041 (0.00549)|         0.00291 (0.00194)|
|2 km            |Coastal not bathing | 138,933|  59,210|  58,912|    -0.00829 (0.00459)|        -0.00250 (0.00167)|
|2 km            |Inland bathing      |   4,305|   4,305|   4,224|    -0.00148 (0.01024)|         0.00302 (0.00372)|
|2 km            |Inland not bathing  | 368,176| 300,536| 299,211|     0.00231 (0.00210)|         0.00184 (0.00080)|
|Intensity       |Positive ≤ median   | 392,878| 276,901| 274,838|    -0.00591 (0.00325)|        -0.00096 (0.00143)|
|Intensity       |Positive > median   | 392,961| 277,986| 275,977|     0.00011 (0.00281)|         0.00114 (0.00123)|
|10 km           |Coastal bathing     |  40,521|  40,521|  40,294|    -0.00124 (0.00521)|         0.00253 (0.00185)|
|10 km           |Coastal not bathing | 238,038| 111,877| 111,263|    -0.00486 (0.00323)|        -0.00048 (0.00116)|
|10 km           |Inland bathing      |   1,688|   1,688|   1,672|     0.01783 (0.01721)|         0.01392 (0.00585)|
|10 km           |Inland not bathing  | 269,071| 247,869| 246,903|     0.00336 (0.00235)|         0.00202 (0.00089)|
|London retained |Coastal bathing     |  37,904|  37,904|  37,715|    -0.00041 (0.00549)|         0.00291 (0.00194)|
|London retained |Coastal not bathing | 138,933|  59,210| 138,610|    -0.02211 (0.00448)|        -0.00593 (0.00177)|
|London retained |Inland bathing      |   4,305|   4,305|   4,224|    -0.00148 (0.01024)|         0.00302 (0.00372)|
|London retained |Inland not bathing  | 368,176| 300,536| 366,785|    -0.00148 (0.00193)|         0.00073 (0.00070)|
|Drop unresolved |Coastal bathing     |  37,904|  37,904|  37,715|    -0.00041 (0.00549)|         0.00291 (0.00194)|
|Drop unresolved |Coastal not bathing | 119,908|  55,497|  55,203|    -0.00920 (0.00473)|        -0.00312 (0.00172)|
|Drop unresolved |Inland bathing      |   4,305|   4,305|   4,224|    -0.00148 (0.01024)|         0.00302 (0.00372)|
|Drop unresolved |Inland not bathing  | 287,910| 268,755| 267,425|     0.00261 (0.00221)|         0.00185 (0.00086)|

## Intensive Margin

### Sales

|Variant         |Stratum             |  Before|   After|       N| Post interaction (SE)| Articles interaction (SE)|
|:---------------|:-------------------|-------:|-------:|-------:|---------------------:|-------------------------:|
|2 km            |Coastal bathing     |  13,310|  13,310|  13,275|    -0.00270 (0.01773)|        -0.00193 (0.00404)|
|2 km            |Coastal not bathing |  25,144|  21,233|  21,160|     0.01251 (0.01263)|         0.00039 (0.00363)|
|2 km            |Inland bathing      |   2,367|   2,367|   2,351|     0.01537 (0.02469)|         0.00096 (0.00626)|
|2 km            |Inland not bathing  | 125,371| 123,208| 122,846|     0.00267 (0.00564)|        -0.00114 (0.00156)|
|Intensity       |Positive ≤ median   |  71,878|  68,964|  68,447|    -0.00332 (0.04709)|         0.01024 (0.01875)|
|Intensity       |Positive > median   |  71,878|  69,358|  68,812|     0.00824 (0.00684)|         0.00020 (0.00195)|
|10 km           |Coastal bathing     |  14,865|  14,865|  14,821|    -0.00506 (0.01594)|        -0.00269 (0.00352)|
|10 km           |Coastal not bathing |  48,965|  43,231|  43,081|     0.01593 (0.00923)|         0.00289 (0.00252)|
|10 km           |Inland bathing      |     812|     812|     809|     0.02746 (0.03008)|         0.00545 (0.00854)|
|10 km           |Inland not bathing  | 101,550| 101,210| 100,929|    -0.00201 (0.00631)|        -0.00258 (0.00177)|
|London retained |Coastal bathing     |  13,310|  13,310|  13,275|    -0.00270 (0.01773)|        -0.00193 (0.00404)|
|London retained |Coastal not bathing |  25,144|  21,233|  25,068|     0.00260 (0.01462)|        -0.00013 (0.00506)|
|London retained |Inland bathing      |   2,367|   2,367|   2,351|     0.01537 (0.02469)|         0.00096 (0.00626)|
|London retained |Inland not bathing  | 125,371| 123,208| 125,004|     0.00270 (0.00562)|        -0.00116 (0.00155)|
|Drop unresolved |Coastal bathing     |  13,310|  13,310|  13,275|    -0.00270 (0.01773)|        -0.00193 (0.00404)|
|Drop unresolved |Coastal not bathing |  24,996|  21,119|  21,046|     0.01114 (0.01254)|         0.00067 (0.00365)|
|Drop unresolved |Inland bathing      |   2,367|   2,367|   2,351|     0.01537 (0.02469)|         0.00096 (0.00626)|
|Drop unresolved |Inland not bathing  | 124,286| 122,211| 121,856|     0.00263 (0.00565)|        -0.00123 (0.00156)|

### Rentals

|Variant         |Stratum             | Before|  After|      N| Post interaction (SE)| Articles interaction (SE)|
|:---------------|:-------------------|------:|------:|------:|---------------------:|-------------------------:|
|2 km            |Coastal bathing     |  2,536|  2,536|  2,448|     0.00323 (0.01648)|         0.00037 (0.00514)|
|2 km            |Coastal not bathing | 10,947|  6,288|  6,164|    -0.00026 (0.00889)|        -0.00210 (0.00219)|
|2 km            |Inland bathing      |    290|    290|    267|     0.05324 (0.03943)|         0.01679 (0.00965)|
|2 km            |Inland not bathing  | 32,651| 31,472| 30,678|     0.00780 (0.00335)|         0.00299 (0.00104)|
|Intensity       |Positive ≤ median   | 19,556| 16,889| 16,111|     0.02127 (0.03656)|         0.01428 (0.01898)|
|Intensity       |Positive > median   | 20,121| 17,554| 16,900|    -0.00014 (0.00427)|         0.00158 (0.00135)|
|10 km           |Coastal bathing     |  2,687|  2,687|  2,577|     0.00184 (0.01538)|         0.00041 (0.00459)|
|10 km           |Coastal not bathing | 17,025| 11,395| 11,095|     0.00341 (0.00560)|         0.00123 (0.00182)|
|10 km           |Inland bathing      |    139|    139|    133|     0.07660 (0.04505)|         0.01572 (0.01932)|
|10 km           |Inland not bathing  | 26,573| 26,365| 25,745|     0.00808 (0.00367)|         0.00276 (0.00120)|
|London retained |Coastal bathing     |  2,536|  2,536|  2,448|     0.00323 (0.01648)|         0.00037 (0.00514)|
|London retained |Coastal not bathing | 10,947|  6,288| 10,817|     0.00031 (0.00961)|        -0.00410 (0.00280)|
|London retained |Inland bathing      |    290|    290|    267|     0.05324 (0.03943)|         0.01679 (0.00965)|
|London retained |Inland not bathing  | 32,651| 31,472| 31,848|     0.00666 (0.00345)|         0.00287 (0.00105)|
|Drop unresolved |Coastal bathing     |  2,536|  2,536|  2,448|     0.00323 (0.01648)|         0.00037 (0.00514)|
|Drop unresolved |Coastal not bathing | 10,871|  6,242|  6,119|    -0.00025 (0.00893)|        -0.00212 (0.00220)|
|Drop unresolved |Inland bathing      |    290|    290|    267|     0.05324 (0.03943)|         0.01679 (0.00965)|
|Drop unresolved |Inland not bathing  | 32,339| 31,225| 30,443|     0.00776 (0.00336)|         0.00294 (0.00105)|

## Baseline hedonic

### Sales

|Variant         |Stratum             |  Before|   After|       N|  Weekly count (SE)|
|:---------------|:-------------------|-------:|-------:|-------:|------------------:|
|2 km            |Coastal bathing     |  13,133|  13,133|  13,097| -0.01531 (0.01357)|
|2 km            |Coastal not bathing |  25,275|  21,364|  21,294|  0.00248 (0.00874)|
|2 km            |Inland bathing      |   2,300|   2,300|   2,283| -0.07047 (0.02928)|
|2 km            |Inland not bathing  | 125,484| 123,321| 122,960| -0.00725 (0.00342)|
|10 km           |Coastal bathing     |  14,650|  14,650|  14,604| -0.01941 (0.01269)|
|10 km           |Coastal not bathing |  49,148|  43,414|  43,268|  0.00565 (0.00623)|
|10 km           |Inland bathing      |     783|     783|     780| -0.10705 (0.05715)|
|10 km           |Inland not bathing  | 101,611| 101,271| 100,990| -0.00958 (0.00369)|
|London retained |Coastal bathing     |  13,133|  13,133|  13,097| -0.01531 (0.01357)|
|London retained |Coastal not bathing |  25,275|  21,364|  25,202| -0.01855 (0.00851)|
|London retained |Inland bathing      |   2,300|   2,300|   2,283| -0.07047 (0.02928)|
|London retained |Inland not bathing  | 125,484| 123,321| 125,118| -0.00745 (0.00341)|
|Drop unresolved |Coastal bathing     |  13,133|  13,133|  13,097| -0.01531 (0.01357)|
|Drop unresolved |Coastal not bathing |  25,167|  21,275|  21,205|  0.00245 (0.00882)|
|Drop unresolved |Inland bathing      |   2,300|   2,300|   2,283| -0.07047 (0.02928)|
|Drop unresolved |Inland not bathing  | 124,552| 122,477| 122,125| -0.00776 (0.00342)|

### Rentals

|Variant         |Stratum             | Before|  After|      N|  Weekly count (SE)|
|:---------------|:-------------------|------:|------:|------:|------------------:|
|2 km            |Coastal bathing     |  2,505|  2,505|  2,419| -0.02139 (0.01611)|
|2 km            |Coastal not bathing | 10,975|  6,316|  6,191| -0.01540 (0.00639)|
|2 km            |Inland bathing      |    278|    278|    255| -0.00895 (0.02224)|
|2 km            |Inland not bathing  | 32,666| 31,487| 30,695| -0.02472 (0.00228)|
|10 km           |Coastal bathing     |  2,650|  2,650|  2,542| -0.02173 (0.01467)|
|10 km           |Coastal not bathing | 17,027| 11,397| 11,097| -0.02257 (0.00401)|
|10 km           |Inland bathing      |    133|    133|    132|  0.00486 (0.05366)|
|10 km           |Inland not bathing  | 26,614| 26,406| 25,787| -0.02368 (0.00254)|
|London retained |Coastal bathing     |  2,505|  2,505|  2,419| -0.02139 (0.01611)|
|London retained |Coastal not bathing | 10,975|  6,316| 10,844| -0.05194 (0.00685)|
|London retained |Inland bathing      |    278|    278|    255| -0.00895 (0.02224)|
|London retained |Inland not bathing  | 32,666| 31,487| 31,865| -0.02540 (0.00230)|
|Drop unresolved |Coastal bathing     |  2,505|  2,505|  2,419| -0.02139 (0.01611)|
|Drop unresolved |Coastal not bathing | 10,935|  6,288|  6,164| -0.01548 (0.00642)|
|Drop unresolved |Inland bathing      |    278|    278|    255| -0.00895 (0.02224)|
|Drop unresolved |Inland not bathing  | 32,398| 31,284| 30,503| -0.02468 (0.00229)|

## Extensive-margin support cells

Counts below use the actual London policy and precede estimator removals. Near/far totals also apply to Articles; their pre/post division is shown for the Post specification. All four cells are nonempty in every variant and market.

### Sales

|Variant         |Stratum             | Near pre| Near post| Far pre| Far post|
|:---------------|:-------------------|--------:|---------:|-------:|--------:|
|2 km            |Coastal bathing     |   26,627|    30,924|  48,532|   57,373|
|2 km            |Coastal not bathing |   47,518|    57,006|  62,140|   75,194|
|2 km            |Inland bathing      |    4,068|     5,111|   9,647|   11,612|
|2 km            |Inland not bathing  |  247,348|   299,214| 350,096|  424,452|
|Intensity       |Positive ≤ median   |   86,001|   133,276| 470,415|  568,631|
|Intensity       |Positive > median   |  101,490|   121,021| 470,415|  568,631|
|10 km           |Coastal bathing     |   29,360|    34,548|  55,063|   65,433|
|10 km           |Coastal not bathing |   94,699|   113,560| 144,601|  174,902|
|10 km           |Inland bathing      |    1,335|     1,487|   3,116|    3,552|
|10 km           |Inland not bathing  |  200,167|   242,660| 267,635|  324,744|
|London retained |Coastal bathing     |   26,627|    30,924|  48,532|   57,373|
|London retained |Coastal not bathing |   57,225|    70,118|  88,933|  108,453|
|London retained |Inland bathing      |    4,068|     5,111|   9,647|   11,612|
|London retained |Inland not bathing  |  260,198|   315,283| 390,828|  472,758|
|Drop unresolved |Coastal bathing     |   26,627|    30,924|  48,532|   57,373|
|Drop unresolved |Coastal not bathing |   44,777|    53,627|  58,411|   71,027|
|Drop unresolved |Inland bathing      |    4,068|     5,111|   9,647|   11,612|
|Drop unresolved |Inland not bathing  |  228,665|   278,340| 317,399|  384,923|

### Rentals

|Variant         |Stratum             | Near pre| Near post| Far pre| Far post|
|:---------------|:-------------------|--------:|---------:|-------:|--------:|
|2 km            |Coastal bathing     |    5,525|     6,593|  11,998|   13,788|
|2 km            |Coastal not bathing |   14,417|    15,540|  14,256|   14,997|
|2 km            |Inland bathing      |      566|       670|   1,337|    1,732|
|2 km            |Inland not bathing  |   63,516|    67,550|  81,570|   87,900|
|Intensity       |Positive ≤ median   |   20,663|    28,660| 109,161|  118,417|
|Intensity       |Positive > median   |   25,113|    25,295| 109,161|  118,417|
|10 km           |Coastal bathing     |    5,916|     7,011|  12,787|   14,807|
|10 km           |Coastal not bathing |   25,266|    27,474|  28,392|   30,745|
|10 km           |Inland bathing      |      175|       252|     548|      713|
|10 km           |Inland not bathing  |   52,667|    55,616|  67,434|   72,152|
|London retained |Coastal bathing     |    5,525|     6,593|  11,998|   13,788|
|London retained |Coastal not bathing |   26,680|    26,932|  45,015|   40,306|
|London retained |Inland bathing      |      566|       670|   1,337|    1,732|
|London retained |Inland not bathing  |   72,779|    74,831| 110,107|  110,459|
|Drop unresolved |Coastal bathing     |    5,525|     6,593|  11,998|   13,788|
|Drop unresolved |Coastal not bathing |   13,541|    14,602|  13,357|   13,997|
|Drop unresolved |Inland bathing      |      566|       670|   1,337|    1,732|
|Drop unresolved |Inland not bathing  |   58,474|    62,129|  71,068|   77,084|

## Sources and reconciliation

The 176 coefficient/SE pairs above cover every stratum and market in all 24 published tables. Each was read from the corresponding saved `fixest` model and checked against its LaTeX column at published precision. Every N matches both the table and the keyed audit CSV; Before and After match the bundle's stored counts. All saved parent-reproduction checks pass. The share of distinct nearest Site Groups with missing coast distance in each prepared market sample is zero in all five audits.

The complete path inventory, variant keys, exclusion counters and reproduction commands are in the [pipeline documentation](../pipeline_documentation.md#heterogeneity-by-local-salience). The [shared vocabulary](../../CONCEPTS.md) and [locked plan](../plans/2026-09-02-001-feat-heterogeneity-by-salience-regressions-plan.md) define the classification contract.

| Analysis / attention | Saved models | Cell-count audit |
|---|---|---|
| Extensive Margin / Post | [did_trends_prior_extensive_salience.rds](../../output/regs/did_trends_prior_extensive_salience.rds) | [did_trends_prior_extensive_salience_cell_counts.csv](../../output/logs/did_trends_prior_extensive_salience_cell_counts.csv) |
| Extensive Margin / Articles | [did_articles_prior_extensive_salience.rds](../../output/regs/did_articles_prior_extensive_salience.rds) | [did_articles_prior_extensive_salience_cell_counts.csv](../../output/logs/did_articles_prior_extensive_salience_cell_counts.csv) |
| Intensive Margin / Post | [did_trends_prior_salience.rds](../../output/regs/did_trends_prior_salience.rds) | [did_trends_prior_salience_cell_counts.csv](../../output/logs/did_trends_prior_salience_cell_counts.csv) |
| Intensive Margin / Articles | [did_articles_prior_salience.rds](../../output/regs/did_articles_prior_salience.rds) | [did_articles_prior_salience_cell_counts.csv](../../output/logs/did_articles_prior_salience_cell_counts.csv) |
| Baseline hedonic / None | [hedonic_count_continuous_prior_salience.rds](../../output/regs/hedonic_count_continuous_prior_salience.rds) | [hedonic_count_continuous_prior_salience_cell_counts.csv](../../output/logs/hedonic_count_continuous_prior_salience_cell_counts.csv) |

This memo is a dated snapshot of published results. The analysis runner does not refresh it automatically.
