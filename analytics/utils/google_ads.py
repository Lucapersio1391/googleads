import sys

from google.api_core import protobuf_helpers
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException


class GoogleAdsManager():
    
    def __init__(self, task, customer_id, logger, ad_group_id=None, ad_id=None, campaign_id=None):
        self.client = GoogleAdsClient.load_from_storage()
        self.task = task
        self.logger = logger
        self.customer_id = customer_id
        self.ad_groupid = ad_group_id
        self.ad_id = ad_id
        self.campaign_id = campaign_id

    def set_service_type(self, service:str, type:str):
        self.service = service
        self.type = type


    def pause_or_unpause_ad(self, action:str):
        ad_group_ad_service = self.client.get_service(self.service)

        ad_group_ad_operation = self.client.get_type(self.type)

        ad_group_ad = ad_group_ad_operation.update
        ad_group_ad.resource_name = ad_group_ad_service.ad_group_ad_path(
            self.customer_id, self.ad_group_id,  self.ad_id
        )
        if action == 'pause' and ad_group_ad.status == self.client.enums.AdGroupStatusEnum.ENABLED:
            ad_group_ad.status = self.client.enums.AdGroupStatusEnum.PAUSED
            mess = "Paused ad group ad {0}."
        elif action == 'unpause' and ad_group_ad.status == self.client.enums.AdGroupStatusEnum.PAUSED:
            ad_group_ad.status = self.client.enums.AdGroupStatusEnum.ENABLED
            mess = "Unpaused ad group ad {0}."
        else:
            mess = "No operation performed."
            
        self.client.copy_from(
            ad_group_ad_operation.update_mask,
            protobuf_helpers.field_mask(None, ad_group_ad._pb),
        )

        ad_group_ad_response = ad_group_ad_service.mutate_ad_group_ads(
            customer_id=self.customer_id, operations=[ad_group_ad_operation]
        )

        self.logger.info(mess.format(ad_group_ad_response.results[0].resource_name))
        

    def update_campaign(self):

        campaign_service = self.client.get_service(self.service)
        campaign_operation = self.client.get_type(self.type)
        campaign = campaign_operation.update

        campaign.resource_name = campaign_service.campaign_path(
            self.customer_id, self.campaign_id
        )

        campaign.status = self.client.enums.CampaignStatusEnum.PAUSED

        campaign.network_settings.target_search_network = False
        self.client.copy_from(
            campaign_operation.update_mask,
            protobuf_helpers.field_mask(None, campaign._pb),
        )

        campaign_response = campaign_service.mutate_campaigns(
            customer_id=self.customer_id, operations=[campaign_operation]
        )

        self.logger.info(f"Updated campaign {campaign_response.results[0].resource_name}.")
    

    def update_keyword(self):
        agc_service = self.client.get_service(self.service)
        ad_group_criterion_operation = self.client.get_type(self.type)

        ad_group_criterion = ad_group_criterion_operation.update
        ad_group_criterion.resource_name = agc_service.ad_group_criterion_path(
            self.customer_id, self.ad_group_id, self.criterion_id
        )
        ad_group_criterion.status = self.client.enums.AdGroupCriterionStatusEnum.ENABLED
        ad_group_criterion.final_urls.append("https://www.google.com")
        self.client.copy_from(
            ad_group_criterion_operation.update_mask,
            protobuf_helpers.field_mask(None, ad_group_criterion._pb),
        )

        agc_response = agc_service.mutate_ad_group_criteria(
            customer_id=self.customer_id, operations=[ad_group_criterion_operation]
        )
        self.logger.info(f"Updated keyword {agc_response.results[0].resource_name}.")

