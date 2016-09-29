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
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Question\Question;
use Doctrine\Common\Persistence\ObjectManager;
use AppBundle\Entity\User;



class LoadReportCommand extends ContainerAwareCommand
{

    /**
     * {@inheritdoc}
     */
    protected function configure()
    {
        $this
            // a good practice is to use the 'app:' prefix to group all your custom application commands
            ->setName('app:loadreport')
            ->setDescription('Loads a report');

    }

    /**
     * This method is executed after interact() and initialize(). It usually
     * contains the logic to execute to complete this command task.
     */
    protected function execute(InputInterface $input, OutputInterface $output)
    {

        $username = 'john_user';
        $file = __DIR__.'/../../../var/data/testdata.txt';

        $em = $this->getContainer()->get('doctrine')->getManager();

        $account = $em->getRepository('AppBundle:Accounts')->findOneBy(['username' => $username]);
        if(!$account) {
            die("Cannot load user");
        } else {
            $service = $this->getContainer()->get('cpr');
            $service->processReport($file, $account);
        }

    }


}
