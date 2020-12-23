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

# Add IOU utility companies.
utilities_df: pd.DataFrame = df[df["is_utility_company"]]
for utility in utilities_df.itertuples():
    utility, created = LoadServingEntity.objects.get_or_create(
        name=utility.name.strip().title(),
        short_name=utility.short_name.strip().upper(),
        state=utility.state.strip()[:2].upper(),
        _parent_utility=None,
    )

# Add CCA LSEs.
cca_df: pd.DataFrame = df[~df["is_utility_company"]]
for cca in cca_df.itertuples():

    short_name = cca.short_name.strip().upper()

    if short_name not in existing_entities:

        name = cca.name.strip().title()
        state = cca.state.strip()[:2].upper()

        if cca.parent_utility:
            parent_utility = LoadServingEntity.objects.get(
                short_name=cca.parent_utility
            )
        else:
            parent_utility = None

        load_serving_entity, created = LoadServingEntity.objects.get_or_create(
            name=name,
            short_name=short_name,
            state=state,
            _parent_utility=parent_utility,
        )

        EmailDomain.objects.get_or_create(
            domain="@" + cca.email_domain,
            load_serving_entity=load_serving_entity,
        )

added = LoadServingEntity.objects.count() - existing_entities.count()
print(
    f'{added if added else "No new"} load serving {"entities" if added > 1 else "entity"} added.'
    "\nTo update existing load serving entities use Django Admin.\n"
)
