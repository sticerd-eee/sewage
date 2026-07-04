# Register of Research Ideas Already on Record

This file is the deduplication baseline for the sewage-spills-and-housing ideation exercise. It
lists every distinct research idea, identification strategy, planned or completed extension,
robustness exercise, dataset acquisition, modelling direction, and explicitly rejected direction
that already appears in the project's notes, todos, plans, reports, meetings, and Overleaf
materials. Idea generators must not re-propose anything below as novel.

**Clarification from Jacopo at Checkpoint 1 (2026-07-04).** Being in this register does not mean
an idea is settled or taken: many entries have not been thought about deeply and most have not
been implemented. Generators may therefore promote a register entry into the candidate pool if it
looks promising — as itself, not only as a substantive extension — provided the candidate cites
the register entry it comes from and adds the case for prioritising it now. The register's role
is honest attribution, not exclusion: nothing below may be presented as new, but anything below
may be presented as worth doing.

Each entry gives a short title, a one-line description, an apparent status (done, in progress,
planned, discussed only, or rejected with reason where stated), and a source pointer. Terms are
spelled out on first use. "CSO" means combined sewer overflow (the relief point on a shared foul-
and-rainwater sewer that discharges to a river when the network is overwhelmed). "WwTW" means
wastewater treatment works. "EDM" means Event Duration Monitor, the sensor that records when and
for how long a storm overflow discharges. "Hedonic" refers to a regression of property price on
property and neighbourhood characteristics plus the environmental exposure of interest.

---

## 1. Identification

1. **Baseline hedonic, full-period quartile dummies.** Regress log sale price or log weekly rent on
   dummy variables for quartiles of total 2021-2023 spill exposure within 250 metres, treating the
   full-period spill total as a long-run local-risk proxy. Status: done. Source:
   `poodle_obsidian_db/projects/sewage/sewage - hedonics.md` (Model 1: Full-Period Aggregation).

2. **Baseline hedonic, spills prior to transaction.** Regress log price or rent on the average daily
   spill count at overflows within 250 metres measured only up to the transaction date, with quarter
   fixed effects, for timing-clean identification. Status: done; coefficient attenuates strongly for
   sales once location and quarter fixed effects are added, more persistent for rentals. Source:
   `sewage - hedonics.md` (Model 2: Spills Prior to Transaction);
   `projects/sewage/docs/reports/2026-06-22-001-intensive-margin-results-tables-report.html`.

3. **Rainfall interacted with predetermined WwTW hydraulic stress as an instrument.** Use rainfall
   shocks interacted with treatment-works loading relative to design capacity to generate exogenous
   within-area, over-time variation in spills; map predicted CSO spills to houses via downstream
   river connectivity; estimate capitalization in a second-stage price equation controlling flexibly
   for rainfall. Status: discussed only / design drafted, not yet estimated. Source:
   `sewage - rainfall x wwtw iv.md`; `docs/meetings/hydraulics instrument/instrument.tex`;
   `sewage - inherited sewer design and spill heterogeneity.md`.

4. **Baseline CSO spill propensity as a second instrument.** Interact rainfall with a CSO's initial
   spill frequency as an additional hydraulic-sensitivity shifter in the first stage. Status:
   discussed only; flagged as correlated with neighbourhood quality so used only with CSO fixed
   effects. Source: `docs/meetings/hydraulics instrument/instrument.tex` (Baseline section).

5. **Elevation-based hydraulic slack instrument.** Instrument spills with each CSO's available
   gravity gradient to the treatment works (elevation difference divided by network pipe distance,
   relative to the minimum self-cleansing slope), normalised within a treatment-works catchment,
   measurable from OS Terrain 5 digital elevation data and Hoffmann et al. catchment distances.
   Status: discussed only, proposed as primary hydraulic share. Source:
   `docs/meetings/hydraulics instrument/instrument.tex` (Elevation-based hydraulic slack);
   scratchpad-converted `docs/meetings/notes for 12 jan 2026.docx` (Share A: Hydraulic Slack Deficit).

6. **Soil permeability (CEH HOST classification) instrument.** Interact rainfall with geologically
   determined soil impermeability, since impermeable clay soils generate more runoff and more
   spilling; enters only through interaction with rainfall, with housing-age controls for the
   development-history correlation. Status: discussed only, proposed as valid secondary share.
   Source: `instrument.tex` (Pumping provisions / Soil permeability); `notes for 12 jan 2026.docx`
   (Share D).

7. **Sub-catchment imperviousness share.** Considered as a hydraulic share (share of contributing
   catchment that is paved). Status: rejected as an identifying share because imperviousness is
   directly correlated with urban density and housing quality; retained only as a first-stage
   control. Source: `notes for 12 jan 2026.docx` (Share C: Sub-Catchment Imperviousness).

8. **CSO network position (upstream/downstream order) as an instrument.** Considered using a CSO's
   position in the sewer network to predict spill intensity. Status: rejected as an identifying
   instrument because the sign of its effect on spill intensity is theoretically ambiguous (weir
   heights determine whether upstream overflows relieve or load downstream ones); retained only as a
   heterogeneity moderator or control. Source: `instrument.tex` (final paragraph of Pumping
   provisions section); `notes for 12 jan 2026.docx` (Share E).

9. **Pumping-provision instrument.** Refine hydraulic exposure with an indicator for whether
   Victorian engineering provided pumping at a CSO, interacted with hydraulic slack and with tidal
   status. Status: discussed only, listed as an optional refinement. Source: `instrument.tex`
   (Pumping provisions / Soil permeability).

10. **Bartik / shift-share design with two identification paths.** Frame the hydraulic instrument
    formally as a shift-share estimator, exploiting either share exogeneity (Victorian engineering
    shares as-good-as-random) or shock exogeneity (rainfall shocks as-good-as-random), and show the
    two paths give consistent price effects. Status: discussed only. Source:
    `notes for 12 jan 2026.docx` (Bartik Design Space: Shares, Shifts, and Unit of Analysis).

11. **Shift candidate: annual rainfall deviation.** Standardised annual catchment rainfall deviation
    from the long-run mean as the primary exogenous shock in the shift-share design. Status:
    discussed only, recommended primary shift. Source: `notes for 12 jan 2026.docx` (Shift 1).

12. **Shift candidate: new residential dwelling / population growth.** Net new housing completions in
    a CSO's contributing catchment as a shock that raises baseline dry-weather sewer flow. Status:
    discussed only, recommended secondary shift under the share-exogeneity path. Source:
    `notes for 12 jan 2026.docx` (Shift 2).

13. **Shift candidate: WwTW capacity changes.** Status: rejected as a shift because capacity changes
    are endogenous to spill performance, infrequent, and anticipated. Source:
    `notes for 12 jan 2026.docx` (Shift 3).

14. **Shift candidate: Water Industry National Environment Programme investment intensity.** Status:
    rejected because this investment is explicitly targeted at poorly performing overflows, the
    opposite of exogenous. Source: `notes for 12 jan 2026.docx` (Shift 4).

15. **Shift candidate: regulatory flow-to-full-treatment multiplier changes.** Status: rejected /
    weak because of insufficient time variation in 2016-2024 and selective application to
    non-compliant works. Source: `notes for 12 jan 2026.docx` (Shift 5).

16. **Choice of unit of analysis for the instrument.** Explicit comparison of house-transaction,
    lower-layer-super-output-area-year, CSO-year, and treatment-works-catchment-year units, with the
    treatment-works-catchment-year unit rejected as too coarse (it destroys the within-catchment
    variation the design relies on). Status: discussed only. Source: `notes for 12 jan 2026.docx`
    (Dimension 3: Unit of Analysis).

17. **EDM monitor rollout as an instrument and information shock.** Exploit the non-random staggered
    installation of Event Duration Monitors (prioritised at bathing waters and Sites of Special
    Scientific Interest) both to instrument observed spills and as an asymmetric information shock,
    with parallel-trends checks across early- and late-monitored overflows. Status: discussed only.
    Source: `notes for 24 nov 2025.docx` (CSO Placement / Potential instruments); `notes for 12 jan
    2026.docx` (CSO Placement).

18. **Historic hydraulic engineering instruments from Victorian design.** Construct predetermined
    instruments for spill propensity from inherited sewer design: sewer-path circuitousness (actual
    length over straight-line distance), gradient, junction density, and capacity pressure (historic
    population over catchment area), including digitising Bazalgette-era and 1865 Metropolitan Board
    of Works drawings for pipe diameters, invert levels, and gradients. Status: discussed only, data
    largely not yet digitised. Source: `notes for 24 nov 2025.docx` and `notes for 12 jan 2026.docx`
    (Historic hydraulic engineering design; Deviations of Sewer Path from Shortest Distance).

19. **Distance to treatment works as an instrument.** Use distance to the WwTW as a predetermined
    proxy for system pressure. Status: discussed only, flagged as explaining pressure but not where a
    system spills. Source: `notes for 24 nov 2025.docx` (Historic Instruments).

20. **Treatment-plant closures and upgrades as shocks.** Use discrete works closures and large
    upgrade projects (Lee Tunnel, Thames Tideway Tunnel, Crossness/Deephams catchment upgrades) as
    plausibly exogenous shocks to spill frequency for affected catchments. Status: discussed only.
    Source: `notes for 24 nov 2025.docx` (Upgrades; Historic Instruments).

21. **Upstream/downstream directional hedonic (river-network exposure mapping).** Distinguish
    hydrologically relevant exposure by whether a spill site lies upstream or downstream of a
    property on the directed river network, using along-river distance and excluding tidal reaches;
    implemented as separate upstream/downstream spill counts, inverse-river-distance-weighted
    counts, and a nearest-site upstream interaction. Status: done, treated as a mechanism/validation
    exercise; evidence mixed and sensitive to fixed effects. Source: `sewage - upstream downstream.md`
    (Models 1-3); `docs/reports/2026-06-22-001-intensive-margin-results-tables-report.html`.

22. **Outfall fixed effects with signed river distance.** Proposed cleaner directional design
    comparing houses upstream versus downstream of the same outfall using overflow fixed effects and
    signed along-river distance, to absorb endogenous site placement. Status: discussed only, flagged
    as the strongest next step for the directional exercise. Source: `sewage - upstream downstream.md`
    (Considerations and Limitations, "Stronger next step").

23. **Staggered event-study / difference-in-differences on spill timing.** Build a house panel with
    binary treatment defined by first quarter a property's exposure exceeds spill-count or
    spill-hours thresholds (mean, median, 75th, 90th, maximum percentiles), estimated with the
    Sun and Abraham (2021) interaction-weighted estimator across radii and fixed-effect choices.
    Status: done (dataset and specifications built). Source:
    `poodle_obsidian_db/projects/sewage/event study dataset construction.md`; `sewage - quick notes.md`
    (2025-02-25).

24. **de Chaisemartin and D'Haultfoeuille difference-in-differences estimator.** Status: tried then
    set aside as too computationally slow for a first pass; advisors recommended off-the-shelf
    estimators instead. Source: `sewage - quick notes.md` (2025-02-25); `docs/meetings/250402_sewage_meeting.md`.

25. **Borusyak, Jaravel and Spiess imputation estimator.** Considered for the extended
    multiple-treatment setting. Status: discussed only. Source: `sewage - quick notes.md` (2025-02-25).

26. **Wooldridge and local-projections (LP-DID) difference-in-differences estimators.** Considered as
    off-the-shelf staggered-adoption estimators; LP-DID recommended by advisors as straightforward.
    Status: discussed only. Source: `sewage - quick notes.md` (2025-02-27);
    `docs/meetings/250402_sewage_meeting.md` (Difference-in-Differences Estimators).

27. **Gridded long-difference design.** Regress the 2021-to-2023 change in mean log price on the
    change in spill exposure across 250-metre grid cells to difference out time-invariant
    confounders. Status: done; coefficients near zero for both sales and rentals. Source:
    `sewage - changes in spill.md` (Model 1: Gridded Long Differences).

28. **Repeat-sales design (Palmquist depreciation-adjusted, Bailey-Muth-Nourse dummies).** Compare
    the same property across consecutive transactions against the change in four-quarter spill
    exposure. Status: done; small and statistically insignificant, limited by small repeat-sales
    sample and high spill persistence. Source: `sewage - changes in spill.md` (Model 2: Repeat Sales).

29. **Combined-versus-separate sewer boundary design (boundary or regression-discontinuity).**
    Explore whether combined/separate sewer system boundaries, and whether separate systems generate
    sanitary sewer overflows in comparison areas, could support a boundary or regression-discontinuity
    design. Status: discussed only, listed as a next action. Source:
    `poodle_obsidian_db/projects/sewage/sewage.md` (Next Actions; Open Questions).

30. **Hydrological boundary discontinuity.** Use hydrological catchment boundaries (top-quartile
    multi-CSO areas) as a discontinuity for exposure. Status: discussed only, brief note. Source:
    `notes for 24 nov 2025.docx` (Causality).

31. **Public attention, pre/post Google Trends peak (August 2022).** Interact spill exposure with a
    post-August-2022 indicator to test whether capitalization strengthens after the national
    attention peak. Status: done. Source: `sewage - news.md` (Model 1);
    `docs/reports/2026-06-22-001-intensive-margin-results-tables-report.html`.

32. **Public attention, log cumulative LexisNexis article coverage.** Interact spill exposure with
    the log of cumulative UK sewage news coverage to the transaction month as a continuous salience
    measure. Status: done. Source: `sewage - news.md` (Model 2);
    `docs/reports/2026-06-22-001-intensive-margin-results-tables-report.html`.

33. **Windowed article-count salience measures (trailing 3, 6, and 12 months).** Add trailing
    fixed-window article-count attention measures alongside the cumulative measure to test whether
    the salience effect is short-lived or robust across windows, for intensive and extensive margins.
    Status: planned / in progress (implementation plan written, GitHub issue #16). Source:
    `docs/plans/2026-06-22-001-feat-windowed-article-salience-measures-plan.md`;
    `docs/reports/2026-06-23-001-windowed-article-salience-results-report.html`.

34. **Lagged public-attention re-estimation for sales (agreement-to-completion mistiming).**
    Systematically lag the attention measures by 3, 6, and 12 months relative to the recorded
    transaction date for house sales, on both the post-peak and cumulative-articles measures and both
    margins, to test whether the null sales result reflects the gap between price agreement and
    registration. Status: planned (GitHub issue #21). Source:
    `docs/plans/2026-06-24-001-feat-lagged-attention-sales-extensive-plan.md`.

35. **Event study around the Google Trends attention peak.** Quarter-level event study relative to
    the attention-peak quarter to test whether the spill-price gradient shifts discretely around the
    peak. Status: done; most interaction coefficients small once fixed effects added. Source:
    `sewage - news.md` (Supporting Checks).

36. **Four-month lagged cumulative-articles specification.** Re-estimate the cumulative-coverage model
    with article counts lagged four months so attention is less mechanically tied to the transaction
    month. Status: done. Source: `sewage - news.md` (Supporting Checks).

37. **Anchor the attention break to an exogenous information event.** Replace the data-mined
    August 2022 breakpoint with an externally defined event (official EDM data release, documentary
    air date, court judgment, or public alert-platform launch). Status: discussed only, flagged as a
    cleaner design. Source: `sewage - news.md` (Considerations, "Endogenous breakpoint selection").

38. **Local geographic attention panel (space-by-time media coverage).** Turn the national monthly
    LexisNexis series into a local space-by-time panel by extracting water-company, river, town, or
    local-authority mentions, so identification can come from within-month variation in local coverage
    with national salience absorbed by month fixed effects. Status: planned (draft summer-RA issue).
    Source: `docs/plans/2026-07-01-001-feat-lexisnexis-llm-relevance-geotag-ra-issue.md`;
    `sewage - news.md` (Considerations, "Mechanism is measured too coarsely").

39. **Large-language-model relevance filtering of LexisNexis articles.** Remove off-topic articles
    from the media-attention series using an LLM (or supervised or rules-based) classifier, and
    rebuild a reliable article-level dataset from the PDF exports. Status: planned (draft summer-RA
    issue). Source: `docs/plans/2026-07-01-001-feat-lexisnexis-llm-relevance-geotag-ra-issue.md`.

40. **Test pre-period spill-price gradient trends across high- and low-exposure areas.** Check
    empirically whether the spill-price gradient trends differently across exposure groups before the
    attention shock (a parallel-trends test for the salience design). Status: discussed only, listed
    as a to-do. Source: `sewage - news.md` (Considerations, first "To do").

---

## 2. Welfare Model

41. **Spatial reallocation counterfactual.** Ask whether a planner could raise welfare by reallocating
    spill discharges (for example forcing them upstream to less populated areas) conditional on total
    volume released, through a spatial model. Status: discussed only, identified as a central
    research question. Source: `docs/meetings/250402_sewage_meeting.md` (Project Framing; Spatial
    Reallocation).

42. **Dry-spill enforcement welfare gains and optimal fines.** Model the welfare gains from properly
    enforcing the ban on dry-day spills and compare implied gains to existing fines. Status:
    discussed only. Source: `docs/meetings/250402_sewage_meeting.md` (Dry Spill Enforcement).

43. **Aggregate welfare gains from eliminating or drastically reducing spills.** Quantify the total
    welfare gain from major infrastructure investment to reduce spills as an input to a cost-benefit
    analysis. Status: discussed only; initially dismissed as ill-defined (no clear counterfactual),
    then reinstated as a first-order input to the national cost-benefit debate. Source:
    `docs/meetings/250402_sewage_meeting.md` (Total Spill Reduction).

44. **Local-versus-regional (global) enforcement externality model.** Compare planner outcomes when
    only local impacts are considered versus when downstream/global impacts are internalised, drawing
    on the dams-and-levees spatial-spillover literature. Status: discussed only; flagged as possibly
    outside the project's comparative advantage and lower priority. Source:
    `docs/meetings/250402_sewage_meeting.md` (Local vs. Regional Planning; Follow-Up Items).

45. **House prices as a summary welfare measure of legacy-infrastructure costs.** Frame estimated
    price capitalization as a summary measure of the buried welfare costs of legacy sewer
    infrastructure, following the Matt Kahn waterway-pollution framing linking economic geography and
    welfare. Status: discussed only (overarching framing). Source:
    `docs/meetings/250402_sewage_meeting.md` (Swati's Perspective; transcript).

46. **Specific infrastructure counterfactuals: storage tanks, Thames Tideway Tunnel, Drainage and
    Wastewater Management Plans.** Named counterfactual interventions to evaluate against the welfare
    model. Status: discussed only (listed). Source: `notes for 24 nov 2025.docx` (Counterfactuals);
    `notes for 12 jan 2026.docx`.

---

## 3. Data and Measurement

47. **Spill severity measures beyond simple counts.** Move beyond count-within-radius to total spill
    duration, distance-weighted duration, and distance-decay weighting of exposure. Status: done in
    part (duration and distance measures constructed) and discussed for further refinement. Source:
    `docs/meetings/250402_sewage_meeting.md` (Spill Data & Severity Measure);
    `docs/meetings/250117_sewage_update.docx`; `sewage - hedonics.md`.

48. **Rainfall-adjusted (dilution) severity measure.** Divide spill duration by contemporaneous
    rainfall to proxy sewage concentration and how damaging a spill would have been, since heavily
    diluted spills may be less noticed. Status: discussed only, requested as an alternative measure.
    Source: `docs/meetings/250402_sewage_meeting.md` (transcript, Danan's dilution suggestion).

49. **Dry-spill identification.** Identify dry-day spills (Environment Agency definition: storm
    overflow operating with no rainfall above 0.25mm on the day and preceding 24 hours) by matching
    spill events to gridded rainfall, to isolate potentially discretionary discharges. Status: in
    progress / partially built (detection script planned in pipeline). Source:
    `poodle_obsidian_db/projects/sewage/sewage - dry spills.md`; `sewage - quick notes.md` (pipeline
    steps 18-19); `docs/meetings/250402_sewage_meeting.md`.

50. **Zoopla / WhenFresh rental and listings data.** Acquire safeguarded property-listings microdata
    (rentals, asking prices, listing attributes, EPC) as a complementary market to Land Registry
    sales, expected to react faster to spills. Status: done (acquired). Source:
    `sewage - data source - zoopla.md`; `sewage - data sources & provenance.md`.

51. **HM Land Registry Price Paid transaction data.** Core house-sale transaction data. Status: done
    (cleaned). Source: `sewage - data source - land registry.md`.

52. **HadUK-Grid rainfall data.** Gridded daily rainfall for the rainfall exposure and
    dry-spill measures. Status: done (acquired). Source: `sewage - data source - haduk rainfall.md`.

53. **Great Britain wastewater catchment dataset (Hoffmann et al. 2025).** Consolidated
    treatment-works catchment boundaries and population-equivalent load, to link CSOs to works and to
    lower-layer-super-output-areas for the hydraulic instrument. Status: identified / acquired
    (zipped in `data/raw`). Source: `notes for 12 jan 2026.docx` (Great Britain wastewater catchment
    dataset); `sewage - rainfall x wwtw iv.md` (catchment mapping "need to confirm").

54. **Urban Waste Water Treatment Regulations capacity data (designated and generated population
    equivalent).** Confirm a WwTW capacity dataset giving designated and actual population equivalent
    to build the loading-ratio stress measure. Status: to find / to confirm. Source:
    `sewage - rainfall x wwtw iv.md` (Data to find or confirm); `instrument.tex` (WwTW Hydraulics).

55. **OS Open Rivers directed river network.** Directed river network for upstream/downstream
    classification and along-river distance. Status: done / in use. Source:
    `sewage - upstream downstream.md`; `sewage - rainfall x wwtw iv.md`.

56. **OS Terrain 5 digital elevation model and LIDAR.** Elevation data for CSO and works elevations
    in the hydraulic-slack instrument. Status: optional refinement, not yet acquired. Source:
    `instrument.tex`; `sewage - rainfall x wwtw iv.md` (Optional refinement data).

57. **Consented discharges database.** Environment Agency permit database used to recover CSO
    location metadata and to help match individual and annual spill records. Status: done (cleaned,
    used in matching pipeline). Source: `sewage - data pipeline.md`; `sewage - quick notes.md`
    (2024-12-18, 2024-12-13).

58. **Tourism outcome data (Booking.com, Airbnb/AirDNA, Inside Airbnb).** Considered as alternative
    outcome data for spill effects on tourism. Status: discussed only / not pursued (no suitable
    historical dataset identified for Booking.com; Airbnb via paid third parties). Source:
    `poodle_obsidian_db/projects/sewage/_archive/data sources.md` (Tourism).

59. **E. coli public-health outcomes (NHS acute-trust incidence).** Test whether monthly E. coli
    incidence at NHS acute-trust level is associated with local spill duration and counts, using
    20-kilometre buffers around trust locations. Status: done (initial report), null result; limited
    by two-company spill coverage, coarse trust-level health data, and absence of dry-spill
    identification. Source: `docs/reports/Sewage Spills report 1 e coli v2.docx`.

60. **Buffer-radius sensitivity (250, 500, 1000, 2000 metres).** Re-estimate the main specifications
    across multiple house-to-site radii as a robustness check. Status: done. Source:
    `sewage - hedonics.md`; `hedonic dataset construction.md`;
    `docs/reports/2026-06-22-001-intensive-margin-results-tables-report.html` (radius robustness
    tables); `docs/meetings/250402_sewage_meeting.md` (buffer sensitivity).

61. **Weekly-unit rescaling of continuous spill coefficients.** Report spill-count and spill-hours
    effects per one additional spill (or spill-hour) per week rather than per daily-average unit, for
    interpretability. Status: planned (branch `jo/rescale-spill-effect`). Source:
    `docs/plans/2026-06-24-002-feat-rescale-spill-effect-plan.md`;
    `docs/solutions/design-patterns/rescale-regressor-at-source-for-interpretable-units.md`.

62. **Quarterly aggregation and absorbing-treatment indicator.** Add quarterly spill aggregation and a
    cumulative (maximum-based) absorbing treatment indicator to the panel. Status: done / in pipeline.
    Source: `sewage - quick notes.md` (2025-04-04, 2025-02-27).

63. **Duration as a proxy for spill volume.** Use recorded spill duration as a proxy for unobserved
    discharge volume, since EDM data give only start/stop times. Status: done (adopted as working
    proxy). Source: `docs/meetings/250402_sewage_meeting.md` (Spill Data & Severity Measure);
    `sewage - inherited sewer design and spill heterogeneity.md` (Caveats).

64. **Google Trends attention data.** Acquire and prepare Google Trends search intensity as a proxy
    for public attention, following the Araujo, Costa and Garg Amazon-fires methodology. Status: done
    (used for the August 2022 peak). Source: `docs/meetings/250402_sewage_meeting.md` (Public
    Awareness); `sewage - data sources & provenance.md` (News Media).

65. **Add 2024 spill data (via the Environment Agency application programming interface) and 2024
    house-price data.** Extend the panel with a further year of spills and prices, including the
    lagged 2024 house prices needed even where 2024 spill data are missing. Status: in progress
    (ingestion pipeline for 2024-onward built). Source: `sewage - quick notes.md` (2025-04-04,
    2026-03-10); `docs/meetings/250402_sewage_meeting.md`.

66. **Extend EDM coverage to more water companies and earlier years.** Broaden beyond the initial
    two-company E. coli sample to all ten English water-and-sewerage companies and back to 2017-2020
    where available. Status: partially done (main housing datasets cover all ten companies 2021-2023).
    Source: `docs/reports/Sewage Spills report 1 e coli v2.docx`; `sewage - data sources & provenance.md`.

---

## 4. Heterogeneity and Mechanisms

67. **Wind-direction interaction to isolate the olfactory (smell) channel.** Interact spill exposure
    with wind direction to test whether the discount is larger for downwind properties, separating
    smell from information channels. Status: discussed only (proposed extension). Source:
    `sewage - hedonics.md` (Missing Considerations, Mechanism of Action).

68. **Buyer-type heterogeneity (investors versus owner-occupiers).** Split the sample by buyer type to
    test whether buy-to-let investors are less sensitive to local sewage disamenity than
    owner-occupiers. Status: discussed only (proposed extension). Source: `sewage - hedonics.md`
    (Missing Considerations, Buyer Heterogeneity).

69. **Nearest-single-site versus multi-site cumulative exposure.** Compare exposure defined by the
    single nearest overflow against exposure summed over all nearby sites, since most properties have
    only one overflow within 250 metres. Status: done (nearest-site specification estimated). Source:
    `sewage - upstream downstream.md` (Model 3); `docs/meetings/250117_sewage_update.docx` (matching
    approaches).

70. **Distance-weighted exposure specifications.** Weight spill exposure by inverse distance (straight-
    line or along-river) so closer overflows count for more. Status: done (inverse-distance and
    inverse-river-distance measures estimated). Source: `hedonic dataset construction.md` (inverse
    distance); `sewage - upstream downstream.md` (Model 2); `docs/meetings/250402_sewage_meeting.md`.

71. **Network-CSO versus treatment-works-overflow mechanism heterogeneity.** Distinguish network
    combined sewer overflows (local pipe/pump/outfall constraints) from treatment-works overflows
    (pass-forward and storage constraints), since pooling them can hide mechanism heterogeneity.
    Status: discussed only (caveat). Source: `sewage - inherited sewer design and spill heterogeneity.md`
    (Caveats).

72. **Time-aggregation of exposure: short window versus annual versus three-year rolling average.**
    Compare short exposure windows (salience) against annual or three-year rolling averages (long-run
    belief formation and capitalization), by analogy to how crime rates rather than single events are
    capitalized. Status: discussed only. Source: `instrument.tex` (Unit of Time);
    `sewage - rainfall x wwtw iv.md` (Considerations).

73. **Big-spill versus small-spill and advertised-versus-unadvertised event studies.** Focus event
    studies on large or publicised spills and ignore small ones, comparing prices before and after
    major or advertised spill events. Status: discussed only (advisor suggestion). Source:
    `docs/meetings/250402_sewage_meeting.md` (Swati's Framework; transcript on collapsing data and
    big-versus-small spills).

74. **Inherited-sewer-design threshold explainer as a mechanism narrative.** Articulate why the same
    rainfall produces different spills across places (inherited hydraulic capacity as a threshold),
    to justify the exclusion restriction and separate the engineering story from identification.
    Status: done as a supporting explainer note. Source:
    `sewage - inherited sewer design and spill heterogeneity.md`.

---

## 5. Other

75. **Flood-risk, distance-to-water, and waterbody-type controls.** Add Environment Agency flood-zone
    designations, distance to water, and waterbody type to separate sewage effects from flood risk and
    river-amenity premia. Status: discussed only (identified as missing controls). Source:
    `sewage - hedonics.md` (Missing Considerations, Flood Risk Confounder and River Amenity Bias);
    `notes for 12 jan 2026.docx` (rainfall-deviation flood control).

76. **Neighbourhood / housing-vintage age controls.** Add controls for neighbourhood and housing-stock
    age, since high-spill areas are often Victorian combined-sewer districts with distinct housing
    stock. Status: discussed only. Source: `sewage - hedonics.md` (Missing Considerations,
    Infrastructure Age); `instrument.tex` (housing-age controls for soil/imperviousness shares).

77. **Energy Performance Certificate attributes as hedonic controls.** Use EPC energy-efficiency and
    rating fields (available in the Zoopla/WhenFresh linkage) as additional property controls.
    Status: available / discussed. Source: `sewage - data source - zoopla.md`; `instrument.tex`
    (controls list).

78. **Inference and standard-error strategy.** Move beyond heteroskedasticity-robust errors to spatial
    and multi-way clustering: cluster at the WwTW or CSO level, use Conley spatial standard errors,
    and account for the national common-time-series structure of the media measure. Status: discussed
    only / partially done (Conley 500-metre and LSOA-clustered errors used in some tables). Source:
    `sewage - rainfall x wwtw iv.md` (Considerations); `sewage - news.md` (Considerations, inference);
    `instrument.tex` (clustering at WwTW/CSO level).

---

## Sources read that contained no register-worthy research content

The following were read in full or scanned and contained only operational, pipeline, formatting, or
administrative material (data-cleaning bug fixes, matching-stage hardening, table layout, folder
structure, meeting templates), with no distinct research idea beyond those already listed above:

- `poodle_obsidian_db/projects/sewage/Full_Matching_Process_Documentation.md`,
  `data construction pipeline.md`, `data_cleaning_pipeline_flowchart.md`, `data_pipeline_flowchart.md`,
  `data_pipeline_flowchart2.md`, `data_pipeline_flowchart3.md`, `data_source_na_analysis.md`,
  `no_full_years_edm_data_analysis.md`, `pipeline_analysis.md`, `site_id_250m_analysis.md`,
  `spill_count_mo_missing_values_analysis.md` (data-pipeline documentation and diagnostics).
- `poodle_obsidian_db/projects/sewage/sewage - data pipeline.md`,
  `sewage - project folder structure.md`, `sewage - data cleaning.md`,
  `sewage - 260221 unique spill dataset changes.md`, `sewage - 260224 spill site not operational.md`,
  `sewage - table notes.md`, `sewage - meeting summary template.md`, `sewage - regulation.md`
  (contains only external chat links), `sewage - literature review.md` (auto-generated table),
  `hedonic dataset construction.md` (construction detail; the analysis ideas it enables are captured
  above).
- All repository todos: `projects/sewage/todos/001` through `008` and `todos/_archive/007` through
  `010` (data-integrity and matching-stage code fixes).
- `projects/sewage/docs/plans/2026-06-10-001`, `2026-06-10-002`, `2026-06-10-004`, and
  `2026-06-26-001` (annual-return-lookup fixes/refactor and regression-table formatting).
- All of `projects/sewage/docs/solutions/` (engineering learnings: script refactors, build errors,
  design patterns, tooling decisions).
- `"Sewage in Our Waters"/notes/knowledge.tex` (an empty background-context template with section
  stubs only).
