from datetime import datetime, timedelta, timezone
from kmd_nexus_client import NexusClientManager
from kmd_nexus_client.tree_helpers import (
    filter_by_path,    
)
from nexus_database_client import NexusDatabaseClient
from odk_tools.tracking import Tracker
from odk_tools.reporting import report
from process.config import get_excel_mapping

proces_navn = "Påmindelse om indberetning til Danmarks statistik"


class NexusService:
    def __init__(self, nexus: NexusClientManager, nexus_database: NexusDatabaseClient, tracker: Tracker):
        self.nexus = nexus
        self.nexus_database = nexus_database
        self.tracker = tracker


    def indsats_kontrol(self, borger: dict):
        regler = get_excel_mapping()
        pathway = self.nexus.borgere.hent_visning(borger=borger)

        if pathway is None:
            raise ValueError(
                f"Kunne ikke finde -Alt for borger {borger['patientIdentifier']['identifier']}"
            )
        
        referencer = self.nexus.borgere.hent_referencer(visning=pathway)
                
        forløb = filter_by_path(
            referencer,
            path_pattern="/Børn og Unge Grundforløb/patientPathwayReference",
            active_pathways_only=True,
        )

        for forløb_item in forløb:
            # Nødvendig da Afgjort er en aktiv state i denne proces, men ikke generelt
            aktive_statistik_indsats_states = [
                "Bestilt",
                "Bevilliget",
                "Anvist",
                "Planlagt, ikke bestilt",
                "Ændret", 
                "Fremtidigt ændret", 
                "Ansøgt", 
                "Afgjort",
                "Iværksat",
                "Etableret"
            ]

            indsats_referencer = filter_by_path(
                referencer,
                path_pattern=f"/Børn og Unge Grundforløb/{forløb_item["name"]}/Indsatser/basketGrantReference",
                active_pathways_only=True,
            )

            filtrerede_indsats_referencer = self.nexus.indsatser.filtrer_indsats_referencer(
                indsats_referencer=indsats_referencer,
                kun_aktive=True,                        
            )
            
            grundindsatser = [item for item in filtrerede_indsats_referencer if item["name"] in regler["Grundindsats"]]            
            statistikindsatser = [item for item in indsats_referencer if item["name"] in regler["Statistikindsats"] and item.get("workflowState", {}).get("name") in aktive_statistik_indsats_states]

            if len(grundindsatser) > 0 and len(statistikindsatser) == 0:
                statistik_indsats = self.opret_statistikindsats(
                    borger=borger,
                    forløbs_navn=forløb_item["name"]
                )
                
                self.opgave_kontrol(
                    borger=borger,
                    statistik_indsats=statistik_indsats,
                    opgave_beskrivelse="Der er fundet indsats uden aktiv Statistikindsats. Robotten har oprettet og ansøgt om en Statistikindsats."
                )
                return
            if len(statistikindsatser) > 1:
                self.opgave_kontrol(
                    borger=borger,
                    statistik_indsats=statistikindsatser[0],
                    opgave_beskrivelse="Der er flere aktive statistikindsatser."
                )
                return
            if len(statistikindsatser) > len(grundindsatser):
                self.opgave_kontrol(
                    borger=borger,
                    statistik_indsats=statistikindsatser[0],
                    opgave_beskrivelse="Der er flere aktive statistikindsatser end indsatser."
                )
                return
            
            aktive_statistikindsatser = [indsats for indsats in statistikindsatser if indsats.get("workflowState", {}).get("name") in ["Iværksat", "Etableret"]]

            if len(aktive_statistikindsatser) == 0:
                continue

            statistik_indsats = self.nexus.hent_fra_reference(aktive_statistikindsatser[0])
            statistik_elementer = self.nexus.indsatser.hent_indsats_elementer(statistik_indsats)
            statistik_leverandør = statistik_elementer.get("supplier", {}).get("supplier", {}).get("name")
            # Leverandørmatch mellem statistik og grundindsats
            for grundindsats_reference in grundindsatser:
                grundindsats = self.nexus.hent_fra_reference(grundindsats_reference)
                grundindsats_elementer = self.nexus.indsatser.hent_indsats_elementer(grundindsats)
                grundindsats_leverandør = grundindsats_elementer.get("supplier", {}).get("supplier", {}).get("name")

                if statistik_leverandør != grundindsats_leverandør:
                    self.opgave_kontrol(
                        borger=borger,
                        statistik_indsats=statistik_indsats,
                        opgave_beskrivelse="Leverandør stemmer ikke overens mellem indsatser og statistikindsats."
                    )
                    pass
        
    def opret_statistikindsats(self, borger: dict, forløbs_navn: str) -> dict:        
        # Afgør om borger er under 18 år
        birth_date = datetime.fromisoformat(borger["birthDate"].replace('Z', '+00:00'))
        today = datetime.now(timezone.utc)
        age_in_years = (today - birth_date).days // 365
        er_18_eller_ældre = age_in_years >= 18

        indsats_navn = "Indberetning til Danmarks Statistik - Anbringelse"

        if er_18_eller_ældre:
            indsats_navn = "Indberetning til Danmarks statistik - Ungestøtte"
        
        return self.nexus.indsatser.opret_indsats(
            borger=borger,
            grundforløb="Børn og Unge Grundforløb",
            forløb=forløbs_navn,
            indsats=indsats_navn,
            felter={
                "workflowRequestedDate": datetime.today()
            },
            oprettelsesform="Ansøg"            
        )


    def opgave_kontrol(self, borger: dict, statistik_indsats: dict, opgave_beskrivelse: str):
        statistik_indsats = self.nexus.hent_fra_reference(statistik_indsats)
        opgave_historik = self.nexus.opgaver.hent_opgave_historik(objekt=statistik_indsats)

        if opgave_historik is not None:
            aktive_opgaver = [opgave for opgave in opgave_historik if opgave["type"]["name"] == "Indsats til Danmarks Statistik" and opgave["workflowState"]["name"] == "Aktiv"]
            
            if len(aktive_opgaver) > 0:
                return
                
        medarbejder = self.hent_medarbejder(borger=borger)

        if medarbejder is None or medarbejder["primaryOrganization"] is None:
            report(
                report_id="paamindelse_om_indberetning_til_danmarks_statistik",
                group="Opgaveoprettelse",
                json={
                    "Cpr": borger["patientIdentifier"]["identifier"],
                    "Fejl": "Kunne ikke finde ansvarlig medarbejder på borgers grundforløb, eller medarbejder har ingen primær organisation."
                }
            )
            self.tracker.track_partial_task(process_name=proces_navn)
            return

        self.nexus.opgaver.opret_opgave(
            objekt=statistik_indsats,
            opgave_type="Indsats til Danmarks Statistik",
            titel="Indsats til Danmarks Statistik",
            ansvarlig_organisation=medarbejder["primaryOrganization"]["name"],
            ansvarlig_medarbejder=medarbejder,
            start_dato=datetime.now().date(),
            forfald_dato=datetime.now().date() + timedelta(days=3),
            beskrivelse=opgave_beskrivelse
        )
                    
        self.tracker.track_task(process_name=proces_navn)

    def hent_medarbejder(self, borger: dict) -> dict | None:
        pathway = self.nexus.borgere.hent_visning(borger=borger)

        if pathway is None:
            raise ValueError(
                f"Kunne ikke finde -Alt for borger {borger['patientIdentifier']['identifier']}"
            )
        
        referencer = self.nexus.borgere.hent_referencer(visning=pathway)

        medarbejder_reference = filter_by_path(
            referencer,
            path_pattern="/Børn og Unge Grundforløb/professionalReference",
            active_pathways_only=True,
        )

        try:
            if len(medarbejder_reference) > 0:
                medarbejder = self.nexus.hent_fra_reference(medarbejder_reference[0])
                medarbejder = self.nexus_database.hent_medarbejder_med_activity_id(
                    medarbejder.get("activityIdentifier", {}).get("activityId", "")
                )
                medarbejder = self.nexus.organisationer.hent_medarbejder_ved_initialer(
                    medarbejder[0].get("primary_identifier", "")
                )

            if medarbejder is not None:
                return medarbejder
   
        except Exception:
            return None

        return None