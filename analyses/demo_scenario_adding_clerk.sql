/* 
SCENARIO: The "Orders Dashboard" needs to have a filter for Clerk Name. 

ACTION: We need to add the "clerk name" field to the dataset that fuels the dashboard. 

HOW DO WE GET THERE? 
    1. Identify where to make the change (add column)
    2. Implement the change in Dev
    3. Test & Document 
    4. Merge & Deploy 
*/

SELECT * 
FROM {{ ref('fct_order_items') }}