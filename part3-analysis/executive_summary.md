# Executive Summary: Business Performance & Analytics Review
**To:** Senior Management -
**From:** Head of Data Infrastructure & Analytics -
**Subject:** 14-Day Performance Review & Strategic Opportunities

## 1. Executive Overview

Over the analyzed 14-day period, Puffy generated roughly **$294,000** in revenue across **38,279 sessions**, resulting in an overall conversion rate (CVR) of **0.76%**.

While topline volume is healthy, we have identified a significant **"Mobile Performance Gap"** and attribution inefficiencies. Our data shows that while Mobile drives the majority of our traffic (66%), it underperforms significantly in conversion compared to Desktop by 36% (.676% vs .924%) . Additionally, Paid Media is currently undervalued if we consider a Last-Click model, hiding its true impact on generating new leads.

## 2. Business Performance Assessment
### What’s Working: High-Value Segments
* **Desktop Efficiency:** Desktop users are our power buyers. specifically **Desktop Safari users** convert at **2.23%**, which is **3x higher** than the site average.
* **Returning User Value:** Returning users are highly valuable. They convert at **2.81%** (vs. 0.64% for new users) and engage more deeply, averaging 3.4 actions per session.
* **Strong AOV:** Our Average Order Value remains robust, averaging between **$900–$1,100** on most days, confirming strong pricing power in the mattress category.

### What’s Concerning: The Mobile Gap
* **Mobile Bleed:** Mobile devices account for **~25,000 sessions** (66% of traffic) but suffer from low conversion rates. **Mobile Chrome** users (our second largest segment at 11,510 sessions) convert at just **0.35%**.
* **Funnel Friction:** We see a steep drop-off at the "Add to Cart" stage. Out of 38,279 sessions, only **2,052 (5.3%)** add an item to the cart. Furthermore, **63% of users who add to cart abandon before checkout** (2,052 carts → 750 checkouts).
* **Revenue Volatility:** Daily revenue is inconsistent, with dips as low as **$12k** (March 4) and peaks up to **$33k** (March 1). Specifically, March 4th saw a crash in AOV to $620, significantly below our $1k average. this could be related to data quality so more investigation is needed.

## 3. Marketing Performance & Attribution
### The "Paid" Discrepancy
Our analysis reveals a critical difference between how we view Paid Media depending on the attribution model. Paid channels are "Openers" that introduce customers to the brand but often lose credit at the final sale.

* **First Click (Awareness):** Paid Marketing is responsible for **$90,448** in revenue.
* **Last Click (Closing):** Paid Marketing is credited with only **$74,721**.
* **Insight:** We are currently **under-crediting Paid Media by ~17%** ($15k) if we only look at Last Click. Cutting spend based on Last Click data would likely harm our top-of-funnel growth.

###The "Direct" Dominance"Direct" traffic accounts for **$146,946** (50% of total revenue). While this suggests strong brand equity, such a high number often hides "Dark Traffic" (untracked social or email clicks). We need to improve our UTM tagging discipline to ensure this isn't masking other channels.

## 4. Strategic Recommendations
1. **Optimize Mobile Checkout:** The disparity between Mobile traffic volume and conversion is our biggest immediate revenue lever. Investigation is needed into the Mobile Chrome user experience, technical bugs or UI friction may be blocking conversions.
2. **Retargeting Focus:** With a **63% cart abandonment rate** and high "Returning User" conversion (2.8%), aggressive email and ad retargeting for cart abandoners will likely yield high ROI.
3. **Adjust Ad Spend Evaluation:** Shift reporting to include **First-Click attribution** for Paid Media. Judging ad spend solely on Last-Click ROI risks choking off the new user pipeline.
4. **Investigate Low AOV Days:** The drop in Average Order Value on March 4th ($620) is an anomaly. We should audit whether this was due to a specific promotion, a stockout of premium mattresses, or a data tracking error like the missing client_id data.

---

### Supporting Charts (Data Reference)
**Chart A: The Mobile vs. Desktop Divide**
*Mobile dominates traffic but fails to convert.*
| Platform | Sessions | Conversion Rate |
| :--- | :--- | :--- |
| **Mobile** | **25,441** | **0.676%** |
| Desktop | 12,229 | 0.924% |


**Chart B: Marketing Contribution (First vs. Last Click)**
*Paid Media drives more value than immediate sales suggest.*
| Channel | First Click Revenue | Last Click Revenue | Variance |
| :--- | :--- | :--- | :--- |
| **Paid** | **$90,448** | $74,721 | **-$15,727** |
| Direct | $148,643 | $146,946 | -$1,697 |
| Referral | $41,346 | $55,899 | +$14,553 |