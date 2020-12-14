import sys
from urllib.error import HTTPError

import pandas as pd
from numpy import nan

from reference.auth_user.models import EmailDomain, LoadServingEntity

# Public url to download load serving entities csv file.
LOAD_SERVICE_ENTITIES_FILE = (
    "https://tvrp.box.com/shared/static/vmr7chhp70ufaholf3i5w4ny5mbytgub.csv"
)

try:
    print("\nDownloading list of load serving entities...")
    df = pd.read_csv(LOAD_SERVICE_ENTITIES_FILE)
    df = df.replace({nan: None})
except HTTPError:
    print("Download Failed!")
    sys.exit(1)
except Exception:
    print("FileFormatError or OtherError!")
    sys.exit(1)

existing_entities = LoadServingEntity.objects.values_list(
    "short_name", flat=True
)

email_columns = [x for x in df.columns if x.startswith("email_domain")]

for item in df.itertuples():

    short_name = item.short_name.strip().upper()

    if short_name not in existing_entities:

        name = item.name.strip().title()
        state = item.state.strip()[:2].upper()

        if item.is_utility_company:
            parent_utility = None
        else:
            parent_utility = LoadServingEntity.objects.get(
                short_name=item.parent_utility
            )

        load_serving_entity = LoadServingEntity.objects.create(
            name=name,
            short_name=short_name,
            state=state,
            _parent_utility=parent_utility,
        )

        for column in email_columns:
            email_domain = getattr(item, column)
            if email_domain:
                EmailDomain.objects.create(
                    domain="@" + email_domain,
                    load_serving_entity=load_serving_entity,
                )

added = LoadServingEntity.objects.count() - existing_entities.count()
print(
    f'{added if added else "No new"} load serving {"entities" if added > 1 else "entity"} added.'
    "\nTo update existing load serving entities use Django Admin.\n"
)
