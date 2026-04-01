from __future__ import annotations

import csv
import json
import tempfile
import unittest
from pathlib import Path

from src.ofac.screening import OFACScreener, extract_identity_key_birth_month_year


class OfacScreeningTests(unittest.TestCase):
    def test_matches_majid_al_zeer_variants(self) -> None:
        screener = OFACScreener()
        screener._entries = [
            {
                "ent_num": "1",
                "name": "AL-ZEER, Majed",
                "aliases": [],
                "program": "SDGT",
                "remarks": "DOB 12 Oct 1959",
                "source": "OFAC SDN",
                "source_id": "1",
            }
        ]

        self.assertTrue(screener.screen_name("Majid al-Zeer", birth_month=10, birth_year=1959))
        self.assertTrue(screener.screen_name("Majed Khalil Al-Zeer", birth_month=10, birth_year=1959))
        self.assertTrue(screener.screen_name("Majed Alzeer", birth_month=10, birth_year=1959))
        self.assertTrue(screener.screen_name("AL-ZEER, Majed Khalil", birth_month=10, birth_year=1959))
        self.assertTrue(screener.screen_name("Majid al-Zeer"))
        self.assertFalse(screener.screen_name("Majid al-Zeer", birth_month=9, birth_year=1959))

    def test_extracts_birth_month_year_from_remarks(self) -> None:
        screener = OFACScreener()
        screener._entries = [
            {
                "ent_num": "1",
                "name": "DOE, John",
                "aliases": [],
                "program": "SDGT",
                "remarks": "DOB 14 Feb 1980; alt. DOB 1981-03-01",
                "source": "OFAC SDN",
                "source_id": "1",
            }
        ]

        self.assertTrue(screener.screen_name("John Doe", birth_month=2, birth_year=1980))
        self.assertTrue(screener.screen_name("John Doe", birth_month=3, birth_year=1981))

    def test_load_uk_csv_extracts_names_and_births(self) -> None:
        rows = [
            {
                "Last Updated": "31/03/2026",
                "Unique ID": "UK001",
                "OFSI Group ID": "123",
                "UN Reference Number": "",
                "Name 1": "Vladimir",
                "Name 2": "Vladimirovich",
                "Name 3": "",
                "Name 4": "",
                "Name 5": "",
                "Name 6": "Putin",
                "Name type": "Primary Name",
                "Alias strength": "",
                "Title": "",
                "Name non-latin script": "",
                "Non-latin script type": "",
                "Non-latin script language": "",
                "Regime Name": "Russia",
                "Designation Type": "Individual",
                "Designation source": "UK",
                "Sanctions Imposed": "Asset freeze",
                "Other Information": "",
                "UK Statement of Reasons": "",
                "Address Line 1": "",
                "Address Line 2": "",
                "Address Line 3": "",
                "Address Line 4": "",
                "Address Line 5": "",
                "Address Line 6": "",
                "Address Postal Code": "",
                "Address Country": "",
                "Phone number": "",
                "Website": "",
                "Email address": "",
                "Date Designated": "",
                "D.O.B": "07/10/1952",
                "Nationality(/ies)": "",
                "National Identifier number": "",
                "National Identifier additional information": "",
                "Passport number": "",
                "Passport additional information": "",
                "Position": "President",
                "Gender": "",
                "Town of birth": "",
                "Country of birth": "",
                "Type of entity": "",
                "Subsidiaries": "",
                "Parent company": "",
                "Business registration number (s)": "",
                "IMO number": "",
                "Current owner/operator (s)": "",
                "Previous owner/operator (s)": "",
                "Current believed flag of ship": "",
                "Previous flags": "",
                "Type of ship": "",
                "Tonnage of ship": "",
                "Length of ship": "",
                "Year Built": "",
                "Hull identification number (HIN)": "",
            },
            {
                "Last Updated": "31/03/2026",
                "Unique ID": "UK001",
                "OFSI Group ID": "123",
                "UN Reference Number": "",
                "Name 1": "V. V.",
                "Name 2": "",
                "Name 3": "",
                "Name 4": "",
                "Name 5": "",
                "Name 6": "Putin",
                "Name type": "Alias",
                "Alias strength": "",
                "Title": "",
                "Name non-latin script": "",
                "Non-latin script type": "",
                "Non-latin script language": "",
                "Regime Name": "Russia",
                "Designation Type": "Individual",
                "Designation source": "UK",
                "Sanctions Imposed": "Asset freeze",
                "Other Information": "",
                "UK Statement of Reasons": "",
                "Address Line 1": "",
                "Address Line 2": "",
                "Address Line 3": "",
                "Address Line 4": "",
                "Address Line 5": "",
                "Address Line 6": "",
                "Address Postal Code": "",
                "Address Country": "",
                "Phone number": "",
                "Website": "",
                "Email address": "",
                "Date Designated": "",
                "D.O.B": "07/10/1952",
                "Nationality(/ies)": "",
                "National Identifier number": "",
                "National Identifier additional information": "",
                "Passport number": "",
                "Passport additional information": "",
                "Position": "",
                "Gender": "",
                "Town of birth": "",
                "Country of birth": "",
                "Type of entity": "",
                "Subsidiaries": "",
                "Parent company": "",
                "Business registration number (s)": "",
                "IMO number": "",
                "Current owner/operator (s)": "",
                "Previous owner/operator (s)": "",
                "Current believed flag of ship": "",
                "Previous flags": "",
                "Type of ship": "",
                "Tonnage of ship": "",
                "Length of ship": "",
                "Year Built": "",
                "Hull identification number (HIN)": "",
            },
        ]
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "uk_sanctions.csv"
            with path.open("w", encoding="utf-8", newline="") as fh:
                fh.write("Report Date: 31-Mar-2026\n")
                writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
                writer.writeheader()
                writer.writerows(rows)
            screener = OFACScreener()
            screener._load_uk_csv(path)

        self.assertTrue(
            screener.screen_name("Vladimir Putin", birth_month=10, birth_year=1952)
        )

    def test_load_france_json_extracts_names_and_births(self) -> None:
        payload = {
            "Publications": {
                "DatePublication": "2026-03-31T11:40:18",
                "PublicationDetail": [
                    {
                        "IdRegistre": 4240,
                        "Nature": "Personne physique",
                        "Nom": "SHILKIN",
                        "RegistreDetail": [
                            {
                                "TypeChamp": "PRENOM",
                                "Valeur": [{"Prenom": "Grigory Vladimirovich"}],
                            },
                            {
                                "TypeChamp": "ALIAS",
                                "Valeur": [{"Alias": "Grigory Shilkin"}],
                            },
                            {
                                "TypeChamp": "DATE_DE_NAISSANCE",
                                "Valeur": [{"DateNaissance": "07.10.1975"}],
                            },
                        ],
                    }
                ],
            }
        }
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "fr_tresor.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            screener = OFACScreener()
            screener._load_france_json(path)

        self.assertTrue(
            screener.screen_name("Grigory Vladimirovich Shilkin", birth_month=10, birth_year=1975)
        )

    def test_parse_german_results_extracts_aliases_and_births(self) -> None:
        html = """
        <div with="*"><span id="message">1 Treffer</span></div>
        <hr /><h3><span style='color:#F00'>100%</span>: (EU 135909) Vladimir Vladimirovich PUTIN - 1952</h3>
        <p>President of the Russian Federation.</p>
        <p><small>/25.02.2022: <em>Name</em>: Vladimir Vladimirovich PUTIN, <em>Geboren</em>: 07.10.1952.</small></p>
        <p>1 Treffer</p>
        """
        screener = OFACScreener()
        entries = screener._parse_german_search_results(html)

        self.assertEqual(len(entries), 1)
        self.assertTrue(
            screener._match_entry(
                "vladimir vladimirovich putin",
                frozenset({"vladimir", "vladimirovich", "putin"}),
                entries[0],
            )
        )
        self.assertEqual(entries[0]["birth_month_years"], {(10, 1952)})

    def test_deduplicates_overlapping_eu_hits_across_sources(self) -> None:
        screener = OFACScreener(enable_remote_sources=True)
        screener._entries = [
            {
                "ent_num": "fr-1",
                "name": "Vladimir Vladimirovich Putin",
                "aliases": [],
                "program": "Direction Generale du Tresor",
                "remarks": "DOB 07.10.1952",
                "source": "Direction Generale du Tresor",
                "source_id": "fr-1",
                "birth_month_years": {(10, 1952)},
            }
        ]
        screener._fetch_german_entries = lambda name: [
            {
                "ent_num": "EU 135909",
                "name": "Vladimir Vladimirovich PUTIN",
                "aliases": [],
                "program": "Germany Finanzsanktionsliste",
                "remarks": "Geboren: 07.10.1952",
                "source": "Germany Finanzsanktionsliste",
                "source_id": "EU 135909",
                "birth_month_years": {(10, 1952)},
            }
        ]

        hits = screener.screen_name("Vladimir Vladimirovich Putin", birth_month=10, birth_year=1952)

        self.assertEqual(len(hits), 1)
        self.assertEqual(hits[0]["source"], "Direction Generale du Tresor")
        self.assertEqual(
            hits[0]["sources"],
            ["Direction Generale du Tresor", "Germany Finanzsanktionsliste"],
        )

    def test_extract_identity_key_birth_month_year_reads_companies_house_keys(self) -> None:
        self.assertEqual(
            extract_identity_key_birth_month_year("ch-name-dob:john_doe:1980-02"),
            (2, 1980),
        )
        self.assertEqual(
            extract_identity_key_birth_month_year("ch-officer:abc123:1975-11"),
            (11, 1975),
        )
        self.assertEqual(
            extract_identity_key_birth_month_year("name:john_doe"),
            (None, None),
        )


if __name__ == "__main__":
    unittest.main()
