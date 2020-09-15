from load.customer.models import OriginFile, CustomerMeter


def run(*args):
    """
    Updates existing CustomerMeter and OriginFile models by populating their
    newly added `total_kwh` and `max_monthly_demand` fields

    Usage:
        - python manage.py runscript load.customer.scripts.add_total_kwh_and_max_monthly_demand_fields
    """
    for Model in [CustomerMeter, OriginFile]:
        print(f"Updating {Model.__name__} models...")
        num_models = Model.objects.count()

        i = 1
        for model in Model.objects.all():
            print(f"Updating model {i}/{num_models} ({model})")
            i += 1
            model.build_aggregate_metrics()
            model.save()
