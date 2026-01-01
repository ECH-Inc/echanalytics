{% docs service_end_date %}
End date of a service. It is derived with following condition:
* if service_status is active or on_hold, set to 31-12-9999
* otherwise, use status_effective_date
{% enddocs %}