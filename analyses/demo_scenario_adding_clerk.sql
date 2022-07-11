/* 
SCENARIO: The "Orders Dashboard" needs to have a filter for Clerk Name. 

ACTION: We need to add the clerk_name field to the dataset that fuels the dashboard. 

HOW DO WE GET THERE? 
1. Identify where to make the change (add column)
2. Implement the change in Dev
3. Validate and Test
4. Document 
5. Deploy 
*/

SELECT * 
FROM {{ ref('fct_order_items') }}