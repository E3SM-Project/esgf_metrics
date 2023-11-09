"""
A script that parses the ESGF Metrics Postgres database for the largest
downloaders based on IP address and performs a lookup for each of them using
the `ipwhois` package.
"""
from __future__ import annotations

from ipwhois import IPWhois, HTTPLookupError
import pandas as pd
from tqdm import tqdm
from typing import Any, Dict, List


from esgf_metrics.database.settings import engine


def get_largest_downloaders() -> pd.DataFrame:
    """Get the IP addresses performed the largest total downloads.

    This function queries the ESGF Metrics Postgres database.

    Returns
    -------
    pd.DataFrame
        A DataFrame of the IP addresses with the largest total downloads.
    """
    df = pd.read_sql(
        """
        SELECT
            lr.ip_address,
            ROUND(CAST(SUM(lr.megabytes) / 1024 AS numeric), 3) AS sum_gb_request,
            ROUND(CAST(SUM(lr.megabytes) / 1048576 AS numeric), 3) AS sum_tb_request,
            lr.project as data_format
        FROM log_request AS lr
        WHERE lr.ip_address != '::1'
        GROUP BY lr.ip_address,
                lr.project
        ORDER BY SUM(megabytes) DESC
        LIMIT 100
        """,
        con=engine,
    )

    return df


def get_user_info(ip_addrs: List[str]) -> pd.DataFrame:
    """Get the user information for each IP address.

    This function uses the `ipwhois` package with the RDAP method to perform
    IP lookups.

    Docs: https://ipwhois.readthedocs.io/en/latest/RDAP.html

    Parameters
    ----------
    ip_addrs : List[str]
        A list of IP addresses.

    Returns
    -------
    pd.DataFrame
        A DataFrame containing lookup information for each IP address.
    """
    rows = []

    for _, ip_addr in enumerate(tqdm(ip_addrs)):
        ip_obj = IPWhois(ip_addr)
        row = {
            "ip_address": ip_addr,
            "name": None,
            "kind": None,
            "asn_country_code": None,
            "asn_description": None,
            "address": None,
            "phone": None,
            "email": None,
            "role": None,
            "title": None,
        }

        try:
            results = ip_obj.lookup_rdap(depth=0)
        except HTTPLookupError as e:
            print(e)
        else:
            row.update(
                {
                    "asn_country_code": results["asn_country_code"],
                    "asn_description": results["asn_description"],
                }
            )
            objects = results["objects"]

            # With depth=0, there will be only 1 object for the main entity,
            # and any sub-entities will be defined in the "entities" attribute.
            # With depth=1, there can be multiple objects, one for each entity.
            obj = list(objects.values())[0]
            contact = obj.get("contact", None)

            if contact is not None:
                row = _get_contact_info(row, contact)

        rows.append(row)

    df = pd.DataFrame(rows)
    return df


def _get_contact_info(
    row: Dict[str, str | None], contact: Dict[str, Any]
) -> Dict[str, str | None]:
    """Get the contact information for the IP address.

    Parameters
    ----------
    row : Dict[str, str  |  None]
        A row entry for an IP address that stores lookup information.
    contact : Dict[str, Any]
        The contact information associated with the IP addressed extracted
        from `ipwhois`.

    Returns
    -------
    Dict[str, str | None]
        The row entry for the IP address with contact information.
    """
    phone = _get_contact_attr(contact["phone"])
    email = _get_contact_attr(contact["email"])

    address = _get_contact_attr(contact["address"])
    if address is not None:
        address = address.replace("\n", " ")
        address = address.replace("\r", "")

    row.update(
        {
            "name": contact["name"],
            "kind": contact["kind"],
            "address": address,
            "phone": phone,
            "email": email,
            "role": contact["role"],
            "title": contact["title"],
        }
    )
    return row


def _get_contact_attr(contact_objs: List[Dict[str, str]]) -> str | None:
    """Get the attribute from the first and only contact objcet.

    This function returns the "value" attr for the first object.

    Parameters
    ----------
    attr_list : List[Dict[str, str]]
        The attribute containing a list of dictionaries.

    Returns
    -------
    str | None
        The contact attribute if it exists.
    """
    if contact_objs is not None:
        return contact_objs[0]["value"]

    return None


if __name__ == "__main__":
    df_largest = get_largest_downloaders()
    ip_addrs = df_largest["ip_address"].unique().tolist()

    df_user_info = get_user_info(ip_addrs)

    df_final = pd.merge(df_user_info, df_largest, how="left", on="ip_address")
    df_final.to_excel("11-9-23-e3sm-llnl-node-user-info.xlsx")
