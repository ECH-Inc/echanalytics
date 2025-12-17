{% docs service_end_date %}
End date of a service. It is derived with following condition:
* if service_status = on_hold, use service_projected_end_date
* if service_status <> on_hold and service_status_date is not null, use service_status_date
* if service_status <> on_hold and service_status_date is null, use service_projected_end_date
* set default value to 31-12-9999 if all above conditions return null value
{% enddocs %}