import logging
import sys

from analytics.utils.google_ads import GoogleAdsManager
from google.ads.googleads.errors import GoogleAdsException
from django.core.management.base import BaseCommand, CommandError


logger = logging.getLogger('main')

def point_9(customer_id, ad_group_id, ad_id, pause_ad, unpause_ad,  update_campaign, criterion_id):

    googleads_manager = GoogleAdsManager(customer_id, ad_group_id, ad_id, pause_ad, unpause_ad, criterion_id)

    if pause_ad or unpause_ad:
        action = 'pause' if pause_ad else 'unpause'
        googleads_manager.set_service_type("AdGroupAdService", "AdGroupAdOperation")
        googleads_manager.pause_or_unpause_ad(action)
    
    elif update_campaign:
        googleads_manager.set_service_type("CampaignService", "CampaignOperation")
        googleads_manager.update_campaign()
        
    elif criterion_id:
        googleads_manager.set_service_type("AdGroupCriterionService", "AdGroupCriterionOperation")
        googleads_manager.update_keyword()


class Command(BaseCommand):

    def handle(self, *args, **kwargs):
        try:

            customer_id = kwargs['customer_id'] 
            ad_group_id = kwargs.get('ad_group_id')
            ad_id = kwargs.get('ad_id')
            pause_ad =  kwargs.get('pause_ad')
            unpause_ad = kwargs.get('unpause_ad')
            update_campaign = kwargs.get('update_campaign')
            criterion_id = kwargs.get('criterion_id')

            point_9(customer_id, ad_group_id, ad_id, pause_ad, unpause_ad,  update_campaign, criterion_id)
        except GoogleAdsException as ex:
            logger.error(
                f'Request with ID "{ex.request_id}" failed with status '
                f'"{ex.error.code().name}" and includes the following errors:'
            )
            for error in ex.failure.errors:
                logger.error(f'\tError with message "{error.message}".')
                if error.location:
                    for field_path_element in error.location.field_path_elements:
                        print(f"\t\tOn field: {field_path_element.field_name}")
            sys.exit(1)
    
    def add_arguments(self, parser):

        parser.add_argument("-c","--customer_id",type=str,required=True,help="The Google Ads customer ID.",)
        parser.add_argument("-a", "--ad_group_id", type=str, help="The ad group ID.")
        parser.add_argument("-i", "--ad_id", type=str, help="The ad ID.")

        group = parser.add_mutually_exclusive_group()

        group_pause_unpause = group.add_mutually_exclusive_group()

        group_pause_unpause.add_argument("-p", "--pause_ad", action='store_true', help="Pause the ad.")
        group_pause_unpause.add_argument("-un", "--unpause_ad", action='store_true', help="Unpause the ad.")

        group.add_argument("-up", "--update_campaign", action='store_true', help="Update the campaign id.")
        group.add_argument("-k","--criterion_id",type=str,help="The keyword ID.",)


    

      