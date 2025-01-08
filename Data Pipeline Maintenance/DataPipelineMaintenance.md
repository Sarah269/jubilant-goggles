
# XYZ Data Engineering Team

This team supports the following pipelines:

<pre>**Business Area:** Profit
    - Unit-level profit needed for experiments
    - Aggregate profit reported to investors
**Business Area:** Growth
    - Aggregate growth reported to investors
    - Daily growth needed for experiments
**Business Area:** Engagement 
    - Aggregate engagement reported to investors </pre>

One-on-one with Stakeholders: Quarterly

## Pipeline: Unit-level profit needed for experiments

**Primary Owner:** Drake Eubanks

**Secondary Owner:** Jake Ferguson

**OnCall Schedule:**
   <pre>Once a month M-F during business hours. No weekends. 
    Primary Owner: Odd months.  Secondary Owner: Even months. Lasts one week.  OnCall hand-off 20-30 minutes. Primary and Secondary will alternate holidays.
    </pre>

## Pipeline: Daily growth needed for experiments

**Primary Owner:** Drake Eubanks

**Secondary Owner:** Jake Ferguson

**OnCall Schedule:**
    <pre>Once a month M-F during business hours. No weekends. 
    Primary Owner: Odd months.  Secondary Owner: Even months. Lasts one week.  OnCall hand-off 20-30 minutes. Primary and Secondary will alternate holidays.
    </pre>

## Contigency Planning for OnCall
- If neither the primary nor secondary owner on a pipeline is available for OnCall coverage, then the available team members shall alternate days to provide coverage.  
- If the primary owner is not available for coverage then the secondary owner shall provide coverage.
- If the secondary owner is not available for coverage, then the primary owner shall provide coverage.


## Runbooks for pipelines that report metrics to Investors

### Pipeline: Aggregate profit reported to investors

**Primary Owner:** Susan Sutton

**Secondary Owner:** Veronica Sloan

**Upstream Owner:**  Finance team

<b>Upstream dataset:</b>
- Revenue
    
**Downstream Owners:**
- Investor Relations
- Dashboard team
    
**Common Issues:**
- Upstream fails to notify regarding adding a new dimension value which causes data quality checks to fail
    
 **SLA:**
- 7 hours after midnight UTC fifth business day of the month
- Contact Investor Relations and Dashboard team if running late
    
**OnCall Schedule:**
    <pre>Once a month M-F during business hours. No weekends. 
    Primary Owner: Odd months.  Secondary Owner: Even months. Lasts one week.  OnCall hand-off 20-30 minutes. Primary and Secondary will alternate holidays.
    </pre>
   

### Pipeline: Aggregate growth reported to investors

**Primary Owner:** Susan Sutton

**Secondary Owner:** Veronica Sloan

**Upstream Owner:** Finance team, Sales team

<b>Upstream dataset:</b>
- Revenue
- Sales
    
**Downstream User:**
- Investor Relations
- Dashboard Team
    
**Common Issues:**
- Upstream fails to notify regarding adding a new dimension value which causes data quality checks to fail
    
**SLA:**
  - 7 hours after midnight UTC fifth business day of the month
- Contact Investor Relations and Dashboard team if running late
    
**OnCall Schedule:**
    <pre>Once a month M-F during business hours. No weekends. 
    Primary Owner: Odd months.  Secondary Owner: Even months. Lasts one week.  OnCall hand-off 20-30 minutes. Primary and Secondary will alternate holidays.
    </pre>


### Pipeline: Aggregate engagement reported to investors

**Primary Owner:** Drake Eubanks

**Secondary Owner:** Veronica Sloan

**Upstream Owner:** Software Engineering team

**Upstream dataset:**
- Events
     
**Downstream User:**
- Investor Relations
- Dashboard team
    
**Common Issues:**
- Upstream does not provide notification for file layout changes
    
**SLA:**
- 7 hours after midnight UTC second business day of the month
- Contact Investor Relations and Dashboard team if running late
    
**OnCall Schedule:**
    <pre>Once a month M-F during business hours. No weekends. 
    Primary Owner: Odd months.  Secondary Owner: Even months. Lasts one week.  OnCall hand-off 20-30 minutes. Primary and Secondary will alternate holidays.
    </pre>


