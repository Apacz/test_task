<?php

/*
 * This file is part of the Symfony package.
 *
 * (c) Fabien Potencier <fabien@symfony.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace AppBundle\Command;

use Symfony\Bundle\FrameworkBundle\Command\ContainerAwareCommand;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;



class TestQueryCommand extends ContainerAwareCommand
{

    /**
     * {@inheritdoc}
     */
    protected function configure()
    {
        $this
            // a good practice is to use the 'app:' prefix to group all your custom application commands
            ->setName('app:testquery')
            ->setDescription('Tests speed of queries');

    }

    protected function execute(InputInterface $input, OutputInterface $output)
    {

        $username = 'john_user';


        $em = $this->getContainer()->get('doctrine')->getManager();

        $account = $em->getRepository('AppBundle:Accounts')->findOneBy(['username' => $username]);


        if(!$account) {
            die("Cannot load user");
        } else {
            $repo = $em->getRepository('AppBundle:CampaignPerformance');
            # Following queries would all be run to generate a real time report for users
            # Normally filters can be added


            $initial_time_start = $time_start = microtime(true);
            $cp = $repo->campaignPerformance($account->getId(), []);
            $time_end = microtime(true);
            $time = $time_end - $time_start;
            echo "Campaign Performance took $time to load ".count($cp)." records\n";


            $time_start = microtime(true);
            $cp = $repo->adgroupStats($account->getId(), []);
            $time_end = microtime(true);
            $time = $time_end - $time_start;
            echo "Ad Group Stats took $time to load ".count($cp)." records\n";

            $time_start = microtime(true);
            $cp = $repo->campaignStats($account->getId(), []);
            $time_end = microtime(true);
            $time = $time_end - $time_start;
            echo "Campaign Stats took $time to load ".count($cp)." records\n";


            $time_start = microtime(true);
            $cp = $repo->skuStats($account->getId(), []);
            $time_end = microtime(true);
            $time = $time_end - $time_start;
            echo "Sku Stats took $time to load ".count($cp)." records\n";


            $final_time_end = microtime(true);
            $time = $final_time_end - $initial_time_start;
            echo "Total : $time \n";



        }

    }


}
