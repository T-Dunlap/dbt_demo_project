select * 
from {{ metrics.calculate(
    metric('revenue'),
    grain='week',
    dimensions=['status_code', 'priority_code']
) }}