---
date: 2025-04-02
company:
  - LSE
project:
  - "[[sewage home]]"
summary: Macro discussion of project
attendees:
  - "[[clare balboni]]"
  - "[[swati dhingra]]"
  - "[[danan dhami]]"
class:
  - meeting
area:
  - "[[meeting MOC]]"
publish: false
---
## Agenda
Discussion of progress of project and next steps

---

## Transcript Notes

### Executive Summary
The meeting aimed to discuss the research direction, potential framing, and specific estimation strategies for a project examining the impact of sewage spills on UK house prices.
Key topics included leveraging the spatial nature of pollution, understanding the role and definition of "dry spills" (potentially discretionary releases), and identifying relevant policy counterfactuals (e.g., spatial reallocation, enforcement of dry spill bans, infrastructure investment). 
Methodologically, the team discussed applying standard hedonic models, addressing potential endogeneity via an instrumental variable (IV) approach using rainfall and hydrology, refining the spill severity measure beyond simple counts, and selecting appropriate difference-in-differences estimators. 
Decisions included adopting Overleaf for collaborative documentation, proceeding with initial hedonic analysis on individual house price data, further investigating dry spills and regulations, and assigning tasks for data documentation, analysis, and literature review.

### Discussion Points
*   **Project Framing & Big Picture Ideas (Led by Clare):**
    *   Inspired by Matt Kahn's work on waterway pollution, linking economic geography and welfare impacts.
    *   Key potential contributions/motivating facts:
        *   Establishing significant house price reductions implies welfare costs.
        *   Highlighting the complex spatial nature where damage may occur far from the release point, motivating modelling beyond simple reduced-form.
        *   Investigating the significance of potentially "discretionary" dry spills versus unavoidable emergency releases.
    *   Proposed Research Questions/Counterfactuals:
        1.  **Spatial Reallocation:** Could a planner improve welfare by changing spill locations (e.g., upstream), conditional on total volume? (Requires understanding engineering/infrastructure constraints).
        2.  **Dry Spill Enforcement:** What are the welfare gains from strictly enforcing regulations against dry spills? (Could inform fines; requires understanding true discretion).
        3.  **Total Spill Reduction:** Quantifying aggregate welfare gains from major infrastructure investment to eliminate/drastically reduce spills (Input for Cost-Benefit Analysis).
        4.  **Local vs. Regional Planning:** Analyzing outcomes if planning considers only local vs. downstream/global impacts (Depends on understanding the actual regulatory/enforcement structure).
*   **Swati's Perspective & Framing:**
    *   Agreed with the focus on quantifying welfare costs associated with legacy infrastructure, viewing house prices as a key indicator.
    *   Emphasized the spatial reallocation question as central.
    *   Initially less focused on the local vs. global angle, seeing it as potentially less novel, but open to it as a motivating tool.
*   **Estimation Strategy & Hedonics:**
    *   **Swati's Framework:** Visualized comparisons: cross-sectional (near vs. far), within-location (adjacent vs. non-adjacent houses), and event-study (before/after spills, potentially differentiating by spill size/publicity). Importance of careful fixed effects specification was highlighted.
    *   **Clare's Recommendations:**
        *   Start with benchmark hedonic models following established literature (e.g., Chay & Greenstone), including standard property characteristics.
        *   Acknowledge likely endogeneity of spill locations (non-random placement). OLS may underestimate effects.
        *   Propose an IV strategy using interactions of upstream rainfall and hydrology/elevation data to instrument for spill exposure.
        *   Conduct sensitivity analyses on buffer radius definitions.
*   **Spill Data & Severity Measure:**
    *   The raw data contains spill *duration*, not volume.
    *   Agreed to use duration as a starting proxy for volume.
    *   Need to refine the spill exposure measure beyond simple counts within a radius. Suggestions:
        *   Use total duration instead of count.
        *   Implement distance weighting (e.g., duration/distance).
        *   Potentially incorporate rainfall data to account for dilution effects (Danan's suggestion).
*   **Dry Spills:**
    *   Environment Agency definition: Spill occurs with <0.25mm rain on the day and day before.
    *   This definition is often disputed by water companies citing catchment area size, residual rainfall, or groundwater.
    *   The actual discretion and legality seem ambiguous and subject to investigation and potential (now monetary) fines. Further research is needed to understand this for policy counterfactuals.
*   **Public Awareness / "Advertised" Spills:**
    *   Discussed the idea that spills might only affect prices if publicly known.
    *   Potential data source: Google Trends (used in papers by Timo Fetzer, Francisco Costa). Acknowledged potential data limitations (indexing).
*   **House Price Data Analysis:**
    *   Need to use individual house price data (not aggregated) for hedonic controls.
    *   Access to Zoopla rental data is pending; rentals might show faster price responses than sales.
    *   House sales data likely has lags. Monthly analysis is likely too noisy. Suggestion to collapse data (e.g., 6-month or annual level) or use event time relative to major spills.
*   **Difference-in-Differences (DiD) Estimators:**
    *   Jacopo currently using a computationally intensive estimator (Chaisemartin & D'Haultfoeuille).
    *   Recommendation: Use standard, off-the-shelf estimators (e.g., Sun & Abraham, LP-DID, simple DiD) for initial analysis to save time. Revisit estimator choice later if needed.

### Action Items
1.  **Set up Overleaf Project:**
    *   **Task:** Create a shared Overleaf project for documentation and collaboration.
    *   **Responsible:** Jacopo
    *   **Deadline:** ASAP
2.  **Draft Project Motivation/Framing Document:**
    *   **Task:** Elaborate on the research framing, questions, and motivation in Overleaf.
    *   **Responsible:** Clare (to start early next week), Swati (to contribute/iterate).
    *   **Deadline:** Ongoing (initial draft by Clare next week).
3.  **Document Estimation Strategy in Overleaf:**
    *   **Task:** Detail the proposed estimation approaches (hedonics, DiD, IV plans) in the Overleaf analysis document, potentially using Swati's framework.
    *   **Responsible:** Jacopo
    *   **Deadline:** Ongoing
4.  **Run Initial Hedonic/OLS & DiD Analyses:**
    *   **Task:** Perform initial regressions using individual house price data, standard controls, and simpler DiD estimators. Populate results in Overleaf.
    *   **Responsible:** Jacopo
    *   **Deadline:** Ongoing
5.  **Document Data Processing:**
    *   **Task:** Create a dedicated section in Overleaf to meticulously document cleaning, merging, and processing steps for all datasets (spills, prices, weather, hydrology, etc.) as they are handled.
    *   **Responsible:** Jacopo
    *   **Deadline:** Ongoing (crucial for reproducibility)
6.  **Prepare for IV Analysis:**
    *   **Task:** Merge weather and hydrology data needed for the IV strategy.
    *   **Responsible:** Danan
    *   **Deadline:** Ongoing
    *   **Dependency:** Clare to suggest specific IV constructions once data is merged.
7.  **Refine Spill Severity Measure:**
    *   **Task:** Propose, implement, and test alternative spill severity measures incorporating duration, distance weighting, and potentially rainfall dilution.
    *   **Responsible:** Jacopo, Danan
    *   **Deadline:** Ongoing
8.  **Research and Document Dry Spills:**
    *   **Task:** Write a summary document (~1 page) in Overleaf explaining dry spills, regulations, enforcement reality, fines, and the ambiguity around discretion.
    *   **Responsible:** Danan
    *   **Deadline:** Ongoing
9.  **Investigate Google Trends Data:**
    *   **Task:** Explore obtaining and preparing Google Trends data related to sewage spills for analysis.
    *   **Responsible:** Danan
    *   **Deadline:** Ongoing
    *   **Dependency:** Clare can assist with contacts (T. Fetzer, F. Costa) if data access is problematic.
10. **Share Cleaned Data File:**
    *   **Task:** Provide a link to the cleaned dataset currently being used.
    *   **Responsible:** Jacopo
    *   **Deadline:** ASAP
11. **Maintain Literature Review Section:**
    *   **Task:** Add relevant papers to a shared literature list in Overleaf as they are found.
    *   **Responsible:** All
    *   **Deadline:** Ongoing
12. **Follow up on Zoopla Data Access:**
    *   **Task:** Pursue access to Zoopla rental data through the safe research mechanism.
    *   **Responsible:** Jacopo
    *   **Deadline:** Ongoing (meeting scheduled for Friday).

### Follow-Up Items
*   **IV Strategy Specification:** Discuss specific IV constructions once weather/hydrology data is merged and Clare has reviewed it.
*   **DiD Estimator Choice:** Re-evaluate the DiD estimator choice based on initial results and potential complexities (e.g., staggered adoption, treatment effect heterogeneity) if necessary.
*   **Local vs. Regional Counterfactual:** Revisit the feasibility and interest of this angle after gaining a better understanding of the regulatory environment and initial findings.
*   **Review Initial Results:** Schedule a follow-up meeting to discuss the findings from the first round of OLS/hedonic and DiD analyses.
*   **Counterfactual Modeling:** Discuss the specific modeling approaches for the proposed counterfactuals (spatial reallocation, dry spill bans) once initial results and contextual understanding (especially regarding dry spills) are more developed.

---

## Transcript
[00:00:00 --> 00:00:07] So the days I go to the airport, I'm just too, I don't know.
[00:00:07 --> 00:00:09] No, it's completely understandable.
[00:00:09 --> 00:00:16] I have basically three, well two categories.
[00:00:16 --> 00:00:23] I've tried to think a little bit about the kind of bigger picture, what might an interesting paper look like
[00:00:23 --> 00:00:28] and particularly that Matt Kahn paper, Sauti, that you sent was really helpful in thinking about the framing.
[00:00:28 --> 00:00:35] So I've tried to sort of brainstorm some ideas on that, which it would be great to talk through at a high level.
[00:00:35 --> 00:00:37] And then there's a series of...
[00:00:37 --> 00:00:40] That's the same by the way.
[00:00:40 --> 00:00:47] Great. And then there's a series of estimation points that I think have come up in the various emails.
[00:00:47 --> 00:00:52] And I've tried to kind of condense that into a bit of a list here.
[00:00:52 --> 00:00:56] Sorry, I should have started by saying thank you both very much for all the work you've done on this.
[00:00:56 --> 00:00:59] It's really moving forward, which is great.
[00:00:59 --> 00:01:05] I'm just trying to kind of organize the proximate tasks on the estimation,
[00:01:05 --> 00:01:10] but also think a little bit about the bigger picture just so that we're kind of directing it.
[00:01:10 --> 00:01:11] It's somewhat directed.
[00:01:11 --> 00:01:17] That's been quite a useful exercise for me largely in defining the bounds of my extensive ignorance.
[00:01:17 --> 00:01:22] So I've kind of written out a list of the kind of...
[00:01:22 --> 00:01:26] First part of that is what's the new motivating evidence that would be really interesting here?
[00:01:26 --> 00:01:31] And the second part of that are what are the implied set of research questions or counterfactuals that I think would be interesting?
[00:01:31 --> 00:01:37] And all of those have sub-bullet points which are, well, this depends on how the structure of this regulation works and so on.
[00:01:37 --> 00:01:40] So I've identified some points within that. I've done it in scribbles here.
[00:01:40 --> 00:01:47] But just in terms of organization, maybe, Jakob, could you set up an overleaf document and add it?
[00:01:47 --> 00:01:50] Sorry, Sveta, do you prefer different...
[00:01:50 --> 00:01:52] No, it's fine.
[00:01:52 --> 00:01:54] Not very accomplished with GitHub yet.
[00:01:54 --> 00:01:56] If you wanted to do that, that would be fine as well.
[00:01:56 --> 00:01:57] Sorry?
[00:01:57 --> 00:02:00] If you wanted to share the screen, if it's already written up, then...
[00:02:00 --> 00:02:05] No, sadly, it looks like this at the moment. My intention was to put it into the overleaf.
[00:02:05 --> 00:02:10] I know some people like working with GitHub. I'm not yet very adept at that.
[00:02:10 --> 00:02:16] So I'd have a slight preference for overleaf if you're indifferent, but I can then put it all in there.
[00:02:16 --> 00:02:22] But do you want to maybe... Are there other things? Sorry, that was just my train of thought.
[00:02:22 --> 00:02:26] I think that sounds really good because mine are pretty specific points.
[00:02:26 --> 00:02:35] I've put some thought into thinking about the big picture, but it's at the moment not at the point where I think you saying things first might help me, actually.
[00:02:35 --> 00:02:40] Okay. My big picture is, as I say, largely an exercise in ignorance.
[00:02:40 --> 00:02:53] But I found the framing of exactly that sentence you pointed out with the last sentence of the abstract in the Matt Caron paper, who, incidentally, I have some working group on something or other with him soon.
[00:02:53 --> 00:02:59] So I'm happy to chat. He would be a really good person to talk to about a lot of issues, I think.
[00:02:59 --> 00:03:11] But the notion that waterway pollution is likely to have important impacts on both economic geography and welfare, and we need to understand the impacts on economic geography to understand the impacts on welfare.
[00:03:11 --> 00:03:19] In terms of what an interesting paper would look like, in trying to crystallize what new motivating evidence might look like on this,
[00:03:19 --> 00:03:26] there's lots of bits of evidence floating around in what you're looking at, Jacob and Dan, which I think will be really useful.
[00:03:26 --> 00:03:34] But the key parts of this, and then we can talk about the implications of research questions, the key parts of this that, from what I understand would be new.
[00:03:34 --> 00:03:44] One is if we find that sewage spills indeed lead to significant reductions in house prices, this suggests that it's important for welfare.
[00:03:44 --> 00:03:59] The second is that this is a complex spatial problem, so much of the damage is likely to be far from the release point because of hydrology and water flow patterns and so on, so we can't probably take a completely reduced form approach to that.
[00:03:59 --> 00:04:09] But seeing that empirically would be important to motivate any kind of a structural exercise to look at, essentially, the kind of spatial geography implications of the problem.
[00:04:09 --> 00:04:22] Third, this is where you'll start to see my ignorance creeping in. As I understand it, a significant share of these spills are likely to be from quote unquote, I'd call them discretionary releases.
[00:04:22 --> 00:04:29] So there seems to be this notion of kind of an emergency release, there's some storms, something you can do, you have to release it, versus these dry spills.
[00:04:29 --> 00:04:43] I'm not sure I know enough yet about the distinction between them and I can put in the overly, you know, if we think this is a helpful approach, a bunch of kind of to do's to do with literature reviews and so on, but certainly when we start to think about what are the interesting set of
[00:04:43 --> 00:04:53] counterfactuals and research questions here, it will be really key to understand how much discretion really there is in tackling this. And I don't have a good sense of that yet.
[00:04:53 --> 00:05:02] I think, Alan, you were looking into dry spills. So maybe we can talk about that. But maybe you could, you know, as part of that tell us a bit about what dry spills are and the conditions under which they happen.
[00:05:02 --> 00:05:16] I put in my list, but also trying to understand the regulations as specifically as possible around when this is allowed and the conditions under which this is something where the firm doesn't have any discretion.
[00:05:16 --> 00:05:33] But that sort of set of motivating facts appears to me to suggest four, I've come up with four, there are probably many more and many more interesting, but four types of questions or counterfactuals here around what, again, my very, very limited sense of what the relevant policy
[00:05:33 --> 00:05:37] levers that a planner might have are.
[00:05:37 --> 00:05:52] So one is conditional on the total volume released. Let's just in the first instance give them the benefit of the doubt and say there's a certain amount to be released. Could there some sort of a spatial product, I think this is probably the most boring, but some version of a could the planner do better by
[00:05:52 --> 00:06:10] reallocating in some way. This would require amongst my kind of sub-bullet to do is a much better understanding of the engineering here. How feasible is it? Does the planner have discretion to release anywhere other than where the excess water is?
[00:06:10 --> 00:06:26] How does the infrastructure work that I don't understand enough about? But, and again, maybe there's more discretion for dry spills here, but is there a spatial reallocation problem here that could improve welfare by forcing it to be upstream as less populated areas
[00:06:26 --> 00:06:36] in this sort of thing? That's something that a spatial model could handle quite easily and would appear to be an interesting question for welfare, if that's something where there's any discretion.
[00:06:36 --> 00:06:51] The second is what would be the welfare gains from enforcement of a ban on, or from proper enforcement of a ban on dry spells? The reason I think that might be interesting is if there are, you know, from a policy perspective, I always come at these things probably too much from the
[00:06:51 --> 00:07:01] perspective of the, what's the interesting policy question here, but, you know, could this be used to inform the relevant fine is?
[00:07:01 --> 00:07:15] We would need to understand, you know, one could model if you were actually to enforce the ban, at least on the, some notion of the discretionary side of this. These would be the implied welfare gains if we think the house price gain is some
[00:07:15 --> 00:07:23] reduced form measure of the overall welfare gains. How does that compare to the existing de facto or de jure fines that are imposed?
[00:07:23 --> 00:07:36] Again, I've put here a lit review on what drives dry spells and whether they're really discretionary. The third I wrote and then deleted and then wrote again, which is, is there any merit in studying the welfare gain from reducing overall
[00:07:36 --> 00:07:44] spills to zero? I thought this is not very interesting because this doesn't make any sense. We don't know what the counterfactual is, if the thing actually overflows and so on.
[00:07:44 --> 00:07:59] But I put it back because I think actually if the big debate here appears to be essentially some version of a cost benefit analysis of the large scale infrastructure investments that are needed to improve the sewer network and whatever, and really
[00:07:59 --> 00:08:10] understanding quantifying the aggregate welfare gains associated with reducing that is a first order input into such a CDA, which doesn't appear to be out there.
[00:08:10 --> 00:08:24] And then my telegraph article incidentally on this topic I found most interesting for its lack of any sort of content. It was sort of saying, we think this might be important, but we don't really know and we certainly can't quantify it, which would sort of
[00:08:24 --> 00:08:26] would be our aim.
[00:08:26 --> 00:08:40] And then the final point, and then I'll stop, sorry, is to, I'm not sure, again, lots of to do bullet points underneath this, whether there's some merit in leaning into the kind of local versus regional enforcement point.
[00:08:40 --> 00:08:49] There's this broader literature, this, and Musvick has some stuff, I saw there's a nice new job market paper on dams and levees and spatial spillover and so on.
[00:08:49 --> 00:09:06] IE something like, how would outcomes and welfare costs be different if the, if the planner were to consider the local impacts versus the global impacts, but I don't know anything really about how far regulation of this or enforcement of the regulations
[00:09:06 --> 00:09:10] or anything to do with the management is local.
[00:09:10 --> 00:09:21] There may be interesting political economy questions there. There's a literature with a bunch of tools we could use if there are but I think we'd need to understand a lot more about whether that's a relevant dimension of this problem or not.
[00:09:21 --> 00:09:33] Is the government just universally bad at doing that, or are they better in areas I don't know where they've got local mayors of aligned and this kind of stuff which may or may not be interesting at all.
[00:09:33 --> 00:09:47] So that would be another set of counterfactuals we could do quite easily through the lens of a simple model would you just, you know, if the planner only considers within my jurisdiction, versus if they consider downstream damages how would their choices be different
[00:09:47 --> 00:10:02] and how would the overall welfare implications be different so this is a super super duper preliminary as I say, shame and Lee, probably very ignorant set of starting points but it helped me to at least try and structure the things I don't know to.
[00:10:02 --> 00:10:08] To kind of think about what, what are the interesting things to look at.
[00:10:08 --> 00:10:16] Maybe there are many many others, I'm happy to kind of write those into an overlay with an set of associated sub bullet point to do.
[00:10:16 --> 00:10:21] But those were kind of high level thoughts I have then there's a bunch of estimation stuff we can talk through if that's helpful.
[00:10:21 --> 00:10:35] I think this is not actually this is really helpful because I'll just give you my thought process my first thought was, you know, is this interesting in and of itself or is this something that you know we're trying to find something, just because we have really
[00:10:35 --> 00:10:38] a very dramatic event here.
[00:10:38 --> 00:10:54] So, so I think my thought process has changed on that originally I thought that because individuals care so much about that this is why the problem is interesting actually now I think it's it's, that's just a reflection of that there are buried welfare costs
[00:10:54 --> 00:11:01] in here that we just don't know how to quantify. So it's much more interesting than I originally thought, economically.
[00:11:01 --> 00:11:13] And here's sort of how I would pitch clear, taking into account what you just said. So, and, and I'm glad we're kind of converging on a similar sort of issue.
[00:11:13 --> 00:11:20] So, the fact that you've got legacy infrastructure, and that it's really expensive to completely approved.
[00:11:20 --> 00:11:34] And it comes with welfare costs attached to it as, you know, as sort of the economy changes and the geography around it changes. I think that's the fundamental question and then asking it within the context of, you know, is, can the regulator do something
[00:11:34 --> 00:11:46] to make improvements, given that are these welfare costs. And I find now that the original sort of thought process I came with that all individuals really care about it seems really interesting.
[00:11:46 --> 00:11:56] And I think that's just a way of quantifying what that welfare cost is so the house prices are just a summary measure of what the community.
[00:11:56 --> 00:12:11] So it's not the overarching framework, you know, it's not as though we necessarily need to find those house price effects, which is what I was coming up with earlier that all we needed to find those house price effects to say that the problem is interesting
[00:12:11 --> 00:12:24] and important. I think it would be excellent if we did try and house price effects simply because it gives us an easy way to quantify the problem, which otherwise becomes a lot harder to do if you were just sort of finding numbers from somewhere else.
[00:12:24 --> 00:12:34] Yeah, so I totally agree with you I think it's a it's the interesting question here is about, is there a speech spatial reallocation that can be done of the switch points.
[00:12:34 --> 00:12:43] And how we quantify that whether through house prices whether through other kind of environmental and economic effects. I'm still sort of uncertain about that.
[00:12:43 --> 00:12:50] So, so I think that bit I agree with you and I think if you write it up and you, and I can sort of add to it if needed.
[00:12:50 --> 00:13:06] And maybe it takes a totally different turn depending on what we find but I don't operate very well in too much of a kind of ether of trying poking around in the data right it's somehow helpful to think a little bit about where it's going so that sounds like
[00:13:06 --> 00:13:23] a local versus global versus regional whatever that bit, I'm not.
[00:13:23 --> 00:13:37] I'm not even thinking about the problem. Yeah, in the way that economists do, but I don't know that literature well enough to really have beyond sort of the usual like my sense is it might not be the comparative advantage here I think it's the trouble is
[00:13:37 --> 00:13:51] that's been done to death a bit on other issues in other contexts. Maybe if it's quantitatively important here, it might be relevant and there's a literature we can do I don't think it's going to be an innovation, but it looks like they're doing really
[00:13:51 --> 00:14:06] well in terms of allocations or completely overlooking the externality or something and maybe we want to look at it but I don't doubt it because it's happening at the level of the water company, which is much broader than just, you know, the local economy effect
[00:14:06 --> 00:14:16] of it whatever. Yeah, no, I'm totally fine with this as a motivating tool to organize the data around.
[00:14:16 --> 00:14:30] So, just sort of bear with me and don't laugh. I will not like you if you do laugh at my attempt to trying to do what I want to do with the pictures if you can give me sharing right.
[00:14:30 --> 00:14:38] I don't know how to make this a default. It's so annoying. I already can.
[00:14:38 --> 00:14:48] I'm going to share something which, again, is my way of trying to make sense of the document that jackables put online and and both of you and Dan and I'm working on.
[00:14:48 --> 00:14:53] So here's the way.
[00:14:53 --> 00:15:02] Can you see this little bit what I'm. So that's the regression jack about that you've written, can you see my highlighted versions as well.
[00:15:02 --> 00:15:12] So I'm going to make sense of what you've done both in terms of the cross sectional analysis as well as the event studies, and if it's not helpful, you know, just feel free to stop me.
[00:15:12 --> 00:15:25] But here's my picture of how I'm thinking of what the house price effects were how the house price effects might come out so this is neighborhood one neighborhood to neighborhood one has the spill site has two houses a one and be one neighbor
[00:15:25 --> 00:15:28] to neighborhood to doesn't have a spill site.
[00:15:28 --> 00:15:45] And then next to each other if you want to believe that. So I think one of the sets of effects that you have in there just pure cross sectional which is saying, let me look at something which is within 250 meters or so of a spill site and let me look at something
[00:15:45 --> 00:15:47] which is which is much further away.
[00:15:47 --> 00:16:05] So, and there we see very clearly that there's definitely house prices, which are lower as you get closer to a spill site. So that's sort of one way of thinking about it. And in that case the exposure variable is just some exposure, I, it's at the neighborhood level.
[00:16:05 --> 00:16:22] And I actually think that that's an important cross sectional variation because it's not clear to me, you know it may be that spill sites, always spill in the same places. So actually the time variation is really not frankly that relevant it's more the actual geography
[00:16:22 --> 00:16:24] of the place which is a fixed feature.
[00:16:24 --> 00:16:38] And then you can get finer and finer so right now you're thinking of the hedonics which is saying can I make a one look a lot more like a to controlling further factors, and so on. And then, you know, you can get fancier and fancier as you want to do it one is just
[00:16:38 --> 00:16:43] compare neighborhood one with neighborhood to with the spill site without the spill site.
[00:16:43 --> 00:17:00] Then, what if the spill site actually becomes relevant only because it's been advertised in the newspaper. So then we can do it before and after, and so on and so forth, we can get fancier do a one versus b one do a one, a one versus b one after an
[00:17:00 --> 00:17:15] after a high spill. So, so this is a little bit how I was organizing my thoughts when I was looking at what you've done. And I think, in some sense organizing it around these lines about these four cuts of the data might help us think about the house price effects
[00:17:15 --> 00:17:30] and then linking it with what Claire is talking about which is you know what's the bigger question here. How can we quantify what the impacts of these things is the plan are doing one approach of spills, whatever spills geography versus the water company
[00:17:30 --> 00:17:38] deciding where to do the spills. So I think that could be something that we can think about them. Is that helping.
[00:17:38 --> 00:17:51] I don't have a diagram as nice as yours but I think exactly this kind of careful consideration of the fixed effects is where a lot of this is going to going to lie so this is, this is really really helpful.
[00:17:51 --> 00:18:00] I'll add this in that was a bit embarrassed to show you my picture but then I realized you know what you might as well deal with my crap.
[00:18:00 --> 00:18:16] I think it probably this is probably going to umbrella in sort of encapsulate everything that I've put here but in case it's helpful as supplementary stuff on this, I put in the literature review folder.
[00:18:16 --> 00:18:30] And I've got some papers which are about as close to the kind of really simplest benchmark version of hedonic papers that I think in it that it looks like basically is what is written the key features of that already there but I think that the idea here in the
[00:18:30 --> 00:18:43] first instance is, let's try and be as close as we can to what is a very very established literature on thinking about different sources of variation for that the chain Greenstone one is sort of the classic.
[00:18:43 --> 00:18:52] So let's try and amongst the set that you're running where we get more and more sophisticated, at least make sure we've got what people will be looking for is the benchmark one.
[00:18:52 --> 00:19:04] I couldn't see this again was in switches documents will be captured but I couldn't see him what you've done, how far property characteristics are captured there but at a minimum we should capture what they're capturing in chain Greenstone that's
[00:19:04 --> 00:19:19] a 20 year old paper so we should be able to do it. We just replicate what they've done and see whether that basic stuff works and I mean this is such a big literature so yeah, we don't need to reinvent the wheel on that stuff but we do need to kind of explore
[00:19:19 --> 00:19:35] carefully how the different sources of variation are affecting it. The other thing to say here is, I'm always quite surprised really in that literature, how much it endogenous exposure matters and I just think that's very very likely to be the case here,
[00:19:35 --> 00:19:49] that the places where they choose to spill are not random in ways that are going to be important that the chain Greenstone paper in a whole bunch of follow ups in lots of context with lots of types of environmental harms find that basically the OLS version of this will show
[00:19:49 --> 00:19:56] you zero when the causal effect is very very far from zero. So let's do, let's do these, the simple version.
[00:19:56 --> 00:20:10] Let's not get too concerned if it if the effects are zero here, there's a gift of an IV because of the kind of rainfall versus downstream type hydrology. Let's do the simple version of it maybe there's something there.
[00:20:10 --> 00:20:26] I don't know if I'd believe it in any case but we can talk in in more detail about how to use that I would suggest we just use some interaction of upstream rainfall and then hydrology based measures about differences in elevation or downstream or whatever which the data
[00:20:26 --> 00:20:40] is actually pretty well again we shouldn't be inventing the kind of geophysical components of this we should just take the natural sciences literature's word for it that you're more exposed if some stuff's going on, that's not going to solve all our endogenous placement
[00:20:40 --> 00:20:48] issues but I've seen a lot worse than that in the evolution literature so I think it's pretty good as a starting point.
[00:20:48 --> 00:20:57] We can talk about how to implement that but it would be great if once we've processed the weather and hydrology data and it's all merged.
[00:20:57 --> 00:21:04] Let me know and I'm happy to sort of suggest some cuts of instruments that might work there.
[00:21:04 --> 00:21:19] I think this was captured pretty well in the in the email chain but the different sets of sensitivity we'd want to check are quite clear here I think so the both in terms of the buffer radius which came up in in the email chain but also the definition of the spill
[00:21:19 --> 00:21:39] radius is quite a bit confused by this account seems to me to be a pretty sort of high level measure ideally this something more correlated with the total volume released which you know I don't know if there's the possibility of sort of some of count times
[00:21:39 --> 00:21:52] the density with the distance weighting factor in front or something again I'm happy to kind of iterate on the specifics of it but I think we'll definitely want to go beyond just the count. It's much worse if it's closer it's much worse if there's more volume released
[00:21:52 --> 00:21:59] it's, you know, there would appear to be lots of ways we can refine that and we should look at sensitivity there.
[00:21:59 --> 00:22:12] And the, the final point is on this kind of advertise versus non advertise point. I don't know if you've had any luck on data sources, there are a couple of recent papers that so Timo Fetzer who was was at the LSE that's now at Warwick.
[00:22:12 --> 00:22:22] He's got a bunch of this kind of papers so that would be a good source of potential data sources, and Francisco Costa.
[00:22:22 --> 00:22:26] I'll put it in the journal.
[00:22:26 --> 00:22:29] So he's one.
[00:22:29 --> 00:22:33] And the other one is Chico.
[00:22:33 --> 00:22:48] So he's got a newish paper on it's on fires and the Amazon is on something completely different but there they've used Google Trends data, which is I understand it was public I know Chico quite well so I can also follow up with him if the data is not publicly
[00:22:48 --> 00:22:56] available I don't know but those data sets should be easy to obtain these days they used in lots of papers. There is something weird.
[00:22:56 --> 00:23:11] There is something weird about the scaling they're done in like on an index or something so there's something a bit weird in in how the publicly released version of the Google Trends data is released, but getting the data shouldn't be at all difficult.
[00:23:11 --> 00:23:19] I mean, they let me know and I can talk to them but those would be the leads, I think, for for looking for the data.
[00:23:19 --> 00:23:28] Yeah, they, I think for Google Trends they use like some sort of like random sampling thing to make the index so it doesn't, it's not always like comparable, depending on.
[00:23:28 --> 00:23:30] Yeah, it's like when you search or something.
[00:23:30 --> 00:23:42] There may be some innovation, those are the only papers that I've seen have been subject to this index issue there may be more recent ones or kind of innovations and data availability that mean there are better data sets again.
[00:23:42 --> 00:23:59] I'm fine within the first instance, sort of following the following the literature but it may be worth a bit of a brutal around in case there are better data sets but I think it'll be fine as a first pass.
[00:23:59 --> 00:24:16] Great. Regarding the spill statistic. So all we actually know from the raw data is when kind of the combined overflow thing opens and when it closes.
[00:24:16 --> 00:24:30] We know each event. So we only have the duration for which the sensor thinks water is spilling out. We don't have any kind of sense of volume.
[00:24:30 --> 00:24:53] I think that's really proportional now I think that seems reasonable to use duration as a measure of volume and then just do this, the sort of weighted some of duration within whatever the radius is where the weighting factor I think is the difficult point.
[00:24:53 --> 00:25:11] Ideally there would be some hydrological mapping function we probably don't want to go there. Maybe just divide it by different radius buffers and put a declining weight and, you know, put a decay function and we test sensitivity but you know fine to do.
[00:25:11 --> 00:25:29] Total count and total count times duration. In the first instance, I would probably do some version of a weighted version of it where the one that's really close by count for slightly more than two involved at this point.
[00:25:29 --> 00:25:34] I think total counted to duration divided by distance or something. Yeah.
[00:25:34 --> 00:25:38] Yeah.
[00:25:38 --> 00:25:46] Is there anything else in the set of emails that was not clear.
[00:25:46 --> 00:25:57] I think if we can just talk about next steps, Claire you and I can iterate on the overleaf document. Great. Can you set up an infrastructure for that please Jacob and invite everybody.
[00:25:57 --> 00:26:03] I will, I'm traveling and whatever till the end of the week but I will prioritize that the beginning of next week.
[00:26:03 --> 00:26:16] Yeah, that's all. Maybe flights and so on.
[00:26:16 --> 00:26:34] Maybe within the overly project if you're able to put a, so if you put the kind of motivation or something document and then in the analysis document if you maybe pull out the kind of the description of the estimation strategy which looks like it's largely already in the
[00:26:34 --> 00:26:51] document. That's what is just shown. And, and then, as you populate each of the four sections. If you pop them in there then we can perhaps chat again once we have some results there.
[00:26:51 --> 00:27:10] Maybe sort of section one is each of the specifications, the kind of first pass or less specifications, and then there's can, as I understand it concurrently you're working on merging the weather and the hydrology data.
[00:27:10 --> 00:27:34] So if you put in a section to with the IV specifications then I can also populate that with some suggestions of how we might construct instruments, whenever that data is merged, and then we can run those as well.
[00:27:34 --> 00:27:50] And then another document within it which is literature, which is a bit ad hoc at the moment so maybe we could just list papers there as we come across.
[00:27:50 --> 00:28:04] Great. Is there anything else that I'm sorry I'm real stickler for process as you can tell but maybe if you have a data document they're not super duper urgent but these things are always so much harder to do six months down the line when you've
[00:28:04 --> 00:28:17] forgotten what you did. If you can, as we come across new data so so obviously this bill's data you're very immersed in at the moment, right up exactly how you're cleaning and choices you're making the purpose thing and then as we get Google trends or
[00:28:17 --> 00:28:32] whatever other data sets just make a new subsection and describe what you did, even if it's a bit overkill and ends up in the data appendix it's from better painful experience very very much harder to back engineer what happened down the line.
[00:28:32 --> 00:28:37] Yes.
[00:28:37 --> 00:28:39] Brilliant.
[00:28:39 --> 00:28:44] Anything else that would be good to discuss today.
[00:28:44 --> 00:28:50] You're free to activate me while players away so you don't lose time.
[00:28:50 --> 00:28:57] Okay, I'm anyway to Friday, so should be able to survive.
[00:28:57 --> 00:29:06] Should be able to kind of respond to emails and stuff I'm just not a little time.
[00:29:06 --> 00:29:14] No, I can show you this kind of like Matt, which basically you can go to like any date and our data.
[00:29:14 --> 00:29:21] And you can see like the weather and all the spills and which ones are dry spills which is already there.
[00:29:21 --> 00:29:24] Yeah, the weather data.
[00:29:24 --> 00:29:32] Yeah, not the river day. Do you know just on the on the sort of intuitively done and do you know what dry spell is. I mean what.
[00:29:32 --> 00:29:47] Yeah, well the definition of the environment agency says it's once they've done their account processing they say the dry spell is when on that day and the day before there's less than 0.25 millimeters of rain.
[00:29:47 --> 00:30:07] But the, is it sort of accepted in the technical definition that under those conditions, the spill is discretionary or are there other circumstances under which the water company just done them any choice because it's I don't know, hot or something else or is it really this is just a breach.
[00:30:07 --> 00:30:25] The water companies is illegal but they'll always say things like different every different spill has like a different train down catchment area. So sometimes they'll say, if we have a bigger catchment area there might be like residual rainfall from way before that leads to the dry spell or underwater
[00:30:25 --> 00:30:44] like underground water sources cause a dry spill. So they always argue with whether it is a dry spill or not. And is there a process for litigating whether or not they win or are they say they say the environment agency like goes to the spill site and investigates it but
[00:30:44 --> 00:31:06] they don't really say about what that investigation is. So is there an outcome from those investigations. Yeah, well, if they have drives built they do issue fines and stuff like that, which we need. But sometimes is defined zero is this, is it ever the case that a dry spill is not punished or deemed by the environment agency to have been legitimate.
[00:31:06 --> 00:31:16] I don't think so but yeah they used to give zero fines, because I think they weren't allowed to actually find monetarily but nowadays they give money, monetary fines.
[00:31:16 --> 00:31:32] Okay, might it be possible once we have our dazzling new leaf infrastructure to just write up a page or so on dry spill and all of these kinds of sorry I know it's sort of embarrassing level of ignorance really but I think it's not well and thinking about the types of
[00:31:32 --> 00:31:41] structures that are even feasible really, which I just don't have good sense of. Okay, I think if you wanted to do volume.
[00:31:41 --> 00:31:53] You could do something like dividing duration by like the rainfall in that area, because I guess that shows like how much has been diluted per hour or whatever it is.
[00:31:53 --> 00:31:56] That's like something we could think about maybe.
[00:31:56 --> 00:32:07] Yeah, that's a good point actually you don't just want how much sewage did they release but how damaging the sewage would have been, which needs to capture the rainfall.
[00:32:07 --> 00:32:14] Because I think, yeah, even for wet spills like this either a lot of rain or, I guess, less rain so it would probably affect.
[00:32:14 --> 00:32:29] Maybe one of between you propose what would be sensible severity measures that incorporate maybe do a version that doesn't account for that and then a version that does or a few versions that do because that that sounds like it's going to be very important,
[00:32:29 --> 00:32:38] you know, maybe even if there's a reasonable volume released if it's all washed away because there's loads of rain and nobody notices and it doesn't affect house prices.
[00:32:38 --> 00:32:47] Yeah, that would be helpful to look at as well.
[00:32:47 --> 00:32:49] Great.
[00:32:49 --> 00:32:52] Okay.
[00:32:52 --> 00:32:55] begins to feel like hopefully this.
[00:32:55 --> 00:32:59] Let's see what we find.
[00:32:59 --> 00:33:07] This was really useful for me at least. Yeah, no, me too. I already understand a lot more after our call.
[00:33:07 --> 00:33:16] Thanks so much for your work on this and let us know when the sort of results or new points that it'd be useful to discuss and set up another call.
[00:33:16 --> 00:33:18] Thanks.
[00:33:18 --> 00:33:22] Just one last thing if you have time.
[00:33:22 --> 00:33:37] So, what I wanted to do and want to hear your opinion on. So right now I think I got too caught up on the difference in difference literature and finding the kind of best estimator.
[00:33:37 --> 00:33:58] It does take a long time to run. And so I aggregated the price at the site level. What I wanted to do was kind of think less about the estimator and actually run this first with actual house for single house prices as the dependent variable, and then
[00:33:58 --> 00:34:08] add controls for the you're going to have to do that for the hedonic for the kind of straightforward hedonic or you can capture. Sorry house characteristics as well.
[00:34:08 --> 00:34:11] Yeah.
[00:34:11 --> 00:34:27] And yes also change the site fixed effects to something larger, especially for the ones where it's only 250 meters radius.
[00:34:27 --> 00:34:41] Yes, possibly you have to see how much variation there is once you're working at the house level on on that how much data do we know how do we have the super data.
[00:34:41 --> 00:34:44] No, so the data.
[00:34:44 --> 00:34:50] The test the safe research thing go move to this Friday.
[00:34:50 --> 00:34:56] So, how to try to and hopefully get access to the data.
[00:34:56 --> 00:35:08] So that has rental values is that right. That has rental values. Yes, I'm a bit the house sales I remain a little bit apprehensive on the timing, I just even if it is a spilly area.
[00:35:08 --> 00:35:25] I would expect that just would be quite a lag in the takes ages to sell a house I'd be amazed if you saw any impact within a couple of months of a spill. I mean, I think rental prices are likely to react faster, but here a bit of thought to the dynamics
[00:35:25 --> 00:35:32] of the data is helpful. I mean, maybe if it's a really bad spill or if they're repeated spills.
[00:35:32 --> 00:35:38] But I feel like you want to do collapse the data down first.
[00:35:38 --> 00:35:47] Just because it's too much I mean month to month variation is probably not going to give you very much of anything.
[00:35:47 --> 00:35:57] So collapse the data down to say a year or so what was the average house price of that price of that house, most likely it will only have one sale in a year.
[00:35:57 --> 00:36:03] And what's the price, sort of before and after the spillage or however you want to.
[00:36:03 --> 00:36:20] I mean, the trouble is I think we've only got three years is that right in the current data set. I mean, we can add the 2020. So, the I saw this bill, there's no spill data for 2024 second add that and the corresponding house prices.
[00:36:20 --> 00:36:33] We should definitely have the house price even if we've only got earlier spill data. The issue is we want the lagged house prices. So, even if we don't have the 2024 spill data, 2024 house price data would be important.
[00:36:33 --> 00:36:39] But I mean month to month doesn't mean anything here. I think minimum six months is fine to collapse.
[00:36:39 --> 00:36:53] I mean, I think we should have a six month window or whatever six month window. That's from the time of the big spill that happens. I feel like somehow you need to have a big spill versus a small spill and we kind of ignore the small spills first and just see whether there's
[00:36:53 --> 00:36:59] Yeah.
[00:36:59 --> 00:37:06] That should also help with relative time terms, not calendar time.
[00:37:06 --> 00:37:22] And on the estimators, I mean, this literature moves every five seconds but I just take an off the shelf one son and Abraham or one of the ones that are, you know, there are these nuances that is Shays Martin stuff and absorbing treatments and law, which
[00:37:22 --> 00:37:36] really kind of immerse myself in revisions on another paper, we can talk about it if you're interested. Certainly in that paper doesn't make the blindest bit of difference which one you use so I think using off the shelf one for now.
[00:37:36 --> 00:37:53] In due course we might, you know, we might refine it because there are nuances as to which estimators most appropriate under different circumstances but I think one of the simple off the shelf estimators will be fine.
[00:37:53 --> 00:37:58] And that's what do you have strong views on estimators here.
[00:37:58 --> 00:38:16] So from my experience with that has been. They all require a fair amount of, I mean the son and Abraham one I think is the most straightforward to understand the ones that I've been using have been typically sort of the problem with anything I've done is that this
[00:38:16 --> 00:38:24] matching becomes really important which house you're comparing with which other house. So, none of them give you the flexibility to do that.
[00:38:24 --> 00:38:30] So, a straightforward different if will actually work pretty well in most circumstances.
[00:38:30 --> 00:38:43] So just go with something like LP DID which is the, the one that are in Dubai and others have been using, it seems fairly straightforward.
[00:38:43 --> 00:38:48] Again, up to you, they don't make that much of a difference.
[00:38:48 --> 00:39:05] I mean right now I'm using the shares my town one, my concern was mainly that just takes a long time to run so yeah I just kind of course it's a, of course it's an approximation but I know certainly my experience of them that hasn't been a huge amount of
[00:39:05 --> 00:39:23] difference across. So I think for a first pass it's not a particularly high return use of your time to be using very slow estimators or bespoke. I think an off the shelf one will be fine and we may wish to refine that later but I don't don't feel that you need to
[00:39:23 --> 00:39:35] spend lots and lots of time and the sort of perfect fit of the estimator here the yeah typically a huge amount of different.
[00:39:35 --> 00:39:46] Actually, one thing that'd be really nice would be that the data if you can just share the link to the cleaned up file. It might help to just look at the data ourselves as well.
[00:39:46 --> 00:39:47] Yes.
[00:39:47 --> 00:39:54] I can do that.
[00:39:54 --> 00:40:07] Is there anything else on the, on the kind of running around the estimation yet but that wasn't clear I mean hopefully that they're having sweaty stock units all written out in terms of the fixed effects but that I think was my main concern on the estimation was that really matters,
[00:40:07 --> 00:40:13] which, you know, the choice of fixed effects and the choice of the level at which you're running it so.
[00:40:13 --> 00:40:23] Sorry I wasn't trying to be prescriptive I was trying to make sense of the results myself and I wasn't quite clear I was getting everything so just alter as you need.
[00:40:23 --> 00:40:32] No, no, that's, it's very helpful and yeah I had, I still have some ideas of what I want to run and stuff.
[00:40:32 --> 00:40:43] If you can, wherever you present a results table or figure if you just put the regression specification, all set of fixed effects and so on then I find it easier to.
[00:40:43 --> 00:40:45] Yeah, I can do that.
[00:40:45 --> 00:40:48] Great. Thank you.
[00:40:48 --> 00:40:51] Okay.
[00:40:51 --> 00:40:52] Okay.
[00:40:52 --> 00:40:54] Thanks so much.
[00:40:54 --> 00:40:56] Thank you.
[00:40:56 --> 00:41:01] Thank you. Bye bye.
[00:41:01 --> 00:41:04] Okay.