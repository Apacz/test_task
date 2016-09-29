<?php

namespace AppBundle\Entity;

use Doctrine\ORM\AbstractQuery;
use Doctrine\ORM\Mapping as ORM;
use Doctrine\ORM\EntityRepository;

class CampaignPerformanceRepository extends EntityRepository {

    private $result = array();
    private function subqueryDQL($account, $filters){



            $qb = $this->getEntityManager()->createQueryBuilder();
            $qb->select('a.id')

                ->from('AppBundle\Entity\CampaignPerformance', 'a')
                ->join('a.accounts', 'b')
                ->join('a.adgroup', 'c')
                ->join('c.campaign', 'd')
                ->join('a.sku', 'e')
//                    ->groupBy('e.sku')

                ->where('a.accounts = :account')
                ->setParameter('account', $account);
            if (array_key_exists('campaign', $filters) && $filters['campaign']) {
                $qb->andWhere('d.   id = :campaign')
                    ->setParameter('campaign', $filters['campaign']);
            }
            if (array_key_exists('adgroup', $filters) && $filters['adgroup']) {
                $qb->andWhere('c.id = :adgroup')
                    ->setParameter('adgroup', $filters['adgroup']);

            }

            if (array_key_exists('matchType', $filters) && $filters['matchType']) {
                $qb->andWhere('a.matchType = :matchType')
                    ->setParameter('matchType', $filters['matchType']);
            }

            if (array_key_exists('startDate', $filters) && $filters['startDate']) {
                $qb->andWhere('a.startDate >= :startDate')
                    ->setParameter('startDate', $filters['startDate']);
            }
            if (array_key_exists('endDate', $filters) && $filters['endDate']) {
                $qb->andWhere('a.startDate <= :endDate')
                    ->setParameter('endDate', $filters['endDate']);
            }
            if (array_key_exists('sku', $filters) && $filters['sku']) {
                $qb->andWhere('e.id = :sku')
                    ->setParameter('sku', $filters['sku']);
            }
            if (array_key_exists('keyword', $filters) && $filters['keyword']) {
                $qb->andWhere('a.keyword = :keyword')
                    ->setParameter('keyword', $filters['keyword']);
            }


            $result = $qb->getQuery()->useQueryCache(true)
                ->useResultCache(true, 3600, 'main')->getDQL();



        return $result;


    }



    private function subquery($account, $filters){

            if(empty($this->result)) {

                $qb = $this->getEntityManager()->createQueryBuilder();
                $qb->select('a1.id')

                    ->from('AppBundle\Entity\CampaignPerformance', 'a1')
                    ->join('a1.accounts', 'b1')
                    ->join('a1.adgroup', 'c1')
                    ->join('c1.campaign', 'd1')
                    ->join('a1.sku', 'e1')
//                    ->groupBy('e.sku')
//                    ->addGroupBy('a.startDate')
//                    ->addGroupBy('c.id')
                    ->where('a1.accounts = :account')
                    ->setParameter('account', $account);
                if (array_key_exists('campaign', $filters) && $filters['campaign']) {
                    $qb->andWhere('d1.   id = :campaign')
                        ->setParameter('campaign', $filters['campaign']);
                }
                if (array_key_exists('adgroup', $filters) && $filters['adgroup']) {
                    $qb->andWhere('c1.id = :adgroup')
                        ->setParameter('adgroup', $filters['adgroup']);

                }

                if (array_key_exists('matchType', $filters) && $filters['matchType']) {
                    $qb->andWhere('a1.matchType = :matchType')
                        ->setParameter('matchType', $filters['matchType']);
                }

                if (array_key_exists('startDate', $filters) && $filters['startDate']) {
                    $qb->andWhere('a1.startDate >= :startDate')
                        ->setParameter('startDate', $filters['startDate']);
                }
                if (array_key_exists('endDate', $filters) && $filters['endDate']) {
                    $qb->andWhere('a1.startDate <= :endDate')
                        ->setParameter('endDate', $filters['endDate']);
                }
                if (array_key_exists('sku', $filters) && $filters['sku']) {
                    $qb->andWhere('e1.id = :sku')
                        ->setParameter('sku', $filters['sku']);
                }
                if (array_key_exists('keyword', $filters) && $filters['keyword']) {
                    $qb->andWhere('a1.keyword = :keyword')
                        ->setParameter('keyword', $filters['keyword']);
                }


                $result = $qb->getQuery()->useQueryCache(true)
                    ->useResultCache(true, 3600, 'main')->getScalarResult();

                $resultNeeded = array_map(function ($value) {
                    return $value['id'];
                }, $result);
                $this->result= $resultNeeded;
            }


        return $this->result;


    }


    public function campaignPerformance($account, $filters) {


//    print_r($this->subquery($account, $filters));

        $cacheName = md5($account.'campainPerformese'.serialize($filters));

//        $qb = $this->defaultQb($account, $filters);
        $qb = $this->getEntityManager()->createQueryBuilder();
        $qb->from('AppBundle\Entity\CampaignPerformance', 'a')
            ->join('a.sku', 'e')
            ->groupBy('e.sku')
            ->addGroupBy('a.startDate')
            ->addSelect('sum(a.impressions) as impressions')
            ->addSelect('sum(a.clicks) as clicks')
            ->addSelect('e.id as id')
            ->addSelect('e.unitCost as baseUnitCost')
            ->addSelect('a.startDate as startDt')

            ->addGroupBy('startDt')
            ->orderBy('startDt', 'asc')
        ->where(
            $qb->expr()->in(
                'a.id', $this->subquery($account, $filters)));



        $results = $qb->getQuery()
            ->useQueryCache(true)
            ->useResultCache(true, 3600, $cacheName)
            ->getResult();
        return $results;

    }

    public function skuStats($account, $filters) {

        $cacheName = md5($account.'skustats'.serialize($filters));
        //        $qb = $this->defaultQb($account, $filters);
        $qb = $this->getEntityManager()->createQueryBuilder();
        $qb ->from('AppBundle\Entity\CampaignPerformance', 'a')
            ->join('a.sku', 'e')
            ->addSelect('sum(a.impressions) as impressions')
            ->addSelect('sum(a.clicks) as clicks')
            ->addSelect('e.id as id')
            ->addSelect('e.unitCost as baseUnitCost')
            ->addSelect('e.sku as sku')
            ->addSelect('e.sku as name')
            ->addSelect('e.img as image')
            ->addSelect('e.asin as asin')
            ->addSelect('e.url as url')
            ->addSelect('e.title as title')
            ->addSelect('e.group as category')
            ->addSelect('e.brand as brand')
            ->groupBy('e.sku')
            ->where(
                $qb->expr()->in(
                    'a.id', $this->subquery($account, $filters)));

        $results = $qb->getQuery()
            ->useQueryCache(true)
            ->useResultCache(true, 3600, $cacheName)
            ->getResult();

        return $results;
    }

    public function adgroupStats($account, $filters) {
        $cacheName = md5($account.'adgroupStats'.serialize($filters));

        //        $qb = $this->defaultQb($account, $filters);
        $qb = $this->getEntityManager()->createQueryBuilder();
        $qb->from('AppBundle\Entity\CampaignPerformance', 'a')
            ->join('a.sku', 'e')
            ->addSelect('sum(a.impressions) as impressions')
            ->addSelect('sum(a.clicks) as clicks')
            ->addSelect('e.id as id')
            ->addSelect('e.unitCost as baseUnitCost')
            ->join('a.adgroup', 'c')
            ->addSelect('c.name as adgroup')
            ->addSelect('c.name as name')
            ->addSelect('c.id as adgroupId')
            ->groupBy('e.sku')
            ->addGroupBy('adgroup')
            ->where(
                $qb->expr()->in(
                    'a.id', $this->subquery($account, $filters)));

        $results = $qb->getQuery()->useQueryCache(true)
            ->useResultCache(true, 3600, $cacheName)
            ->getResult();


        return $results;
    }


    public function campaignStats($account, $filters) {
        $cacheName = md5($account.'campaignStats'.serialize($filters));

        //        $qb = $this->defaultQb($account, $filters);
        $qb = $this->getEntityManager()->createQueryBuilder();
        $qb->from('AppBundle\Entity\CampaignPerformance', 'a')
            ->join('a.sku', 'e')
            ->addSelect('sum(a.impressions) as impressions')
            ->addSelect('sum(a.clicks) as clicks')
            ->addSelect('e.id as id')
            ->addSelect('e.unitCost as baseUnitCost')
            ->join('a.adgroup', 'c')
            ->join('c.campaign', 'd')
            ->addSelect('d.name as campaign')
            ->addSelect('d.name as name')
            ->addSelect('d.id as campaignId')
            ->groupBy('e.sku')
            ->addGroupBy('campaignId')
            ->orderBy('campaign')
        ->where(
            $qb->expr()->in(
                'a.id', $this->subquery($account, $filters)));


        $results = $qb->getQuery()->useQueryCache(true)
            ->useResultCache(true, 3600, $cacheName)
            ->getResult();

        return $results;
    }


    public function campaignPerformance2($account, $filters) {




        $cacheName = md5($account.'campainPerformese'.serialize($filters));

        $qb = $this->defaultQb($account, $filters);
        $qb->addSelect('a.startDate as startDt')
            ->addGroupBy('startDt')

            ->orderBy('startDt', 'asc');
        echo 'finish';
        print_r($qb->getQuery()->getDQL());
        echo 'finish';
        $results = $qb->getQuery()
            ->useQueryCache(true)
            ->useResultCache(true, 3600, $cacheName)
            ->getResult();
        return $results;

    }

    public function skuStats2($account, $filters) {

        $cacheName = md5($account.'skustats'.serialize($filters));
        $qb = $this->defaultQb($account, $filters);
        $qb->addSelect('e.sku as sku')
            ->addSelect('e.sku as name')
            ->addSelect('e.img as image')
            ->addSelect('e.asin as asin')
            ->addSelect('e.url as url')
            ->addSelect('e.title as title')
            ->addSelect('e.group as category')
            ->addSelect('e.brand as brand');

        $results = $qb->getQuery()
            ->useQueryCache(true)
            ->useResultCache(true, 3600, $cacheName)
            ->getResult();

        return $results;
    }

    public function adgroupStats2($account, $filters) {
        $cacheName = md5($account.'adgroupStats'.serialize($filters));

        $qb = $this->defaultQb($account, $filters);
        $qb->addSelect('c.name as adgroup')
            ->addSelect('c.name as name')
            ->addSelect('c.id as adgroupId')
            ->addGroupBy('adgroup');

        $results = $qb->getQuery()->useQueryCache(true)
            ->useResultCache(true, 3600, $cacheName)
            ->getResult();


        return $results;
    }


    public function campaignStats2($account, $filters) {
        $cacheName = md5($account.'campaignStats'.serialize($filters));

        $qb = $this->defaultQb($account, $filters);
        $qb->addSelect('d.name as campaign')
            ->addSelect('d.name as name')
            ->addSelect('d.id as campaignId')
            ->addGroupBy('campaignId')
            ->orderBy('campaign');



        $results = $qb->getQuery()->useQueryCache(true)
            ->useResultCache(true, 3600, $cacheName)
            ->getResult();

        return $results;
    }



    private function defaultQb($account, $filters) {


        $qb = $this->getEntityManager()->createQueryBuilder();
        $qb->
            select('sum(a.impressions) as impressions')
            ->addSelect('sum(a.clicks) as clicks')
            ->addSelect('e.id as id')
            ->addSelect('e.unitCost as baseUnitCost')
            ->from('AppBundle\Entity\CampaignPerformance', 'a')
            ->join('a.accounts', 'b')
            ->join('a.adgroup', 'c')
            ->join('c.campaign', 'd')
            ->join('a.sku', 'e')
            ->groupBy('e.sku')
            ->where('a.accounts = :account')
            ->setParameter('account', $account);
        if(array_key_exists('campaign', $filters) && $filters['campaign']) {
            $qb->andWhere('d.   id = :campaign')
                ->setParameter('campaign', $filters['campaign']);
        }
        if(array_key_exists('adgroup', $filters) && $filters['adgroup']) {
            $qb->andWhere('c.id = :adgroup')
                ->setParameter('adgroup', $filters['adgroup']);

        }

        if(array_key_exists('matchType', $filters) && $filters['matchType']) {
            $qb->andWhere('a.matchType = :matchType')
                ->setParameter('matchType', $filters['matchType']);
        }

        if(array_key_exists('startDate', $filters) && $filters['startDate']) {
            $qb->andWhere('a.startDate >= :startDate')
                ->setParameter('startDate', $filters['startDate']);
        }
        if(array_key_exists('endDate', $filters) && $filters['endDate']) {
            $qb->andWhere('a.startDate <= :endDate')
                ->setParameter('endDate', $filters['endDate']);
        }
        if(array_key_exists('sku', $filters) && $filters['sku']) {
            $qb->andWhere('e.id = :sku')
                ->setParameter('sku', $filters['sku']);
        }
        if(array_key_exists('keyword', $filters) && $filters['keyword']) {
            $qb->andWhere('a.keyword = :keyword')
                ->setParameter('keyword', $filters['keyword']);
        }

        return $qb;

    }


}
