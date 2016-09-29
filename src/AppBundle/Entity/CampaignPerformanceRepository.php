<?php

namespace AppBundle\Entity;

use Doctrine\ORM\Mapping as ORM;
use Doctrine\ORM\EntityRepository;

class CampaignPerformanceRepository extends EntityRepository {


    public function campaignPerformance($account, $filters) {

        $qb = $this->defaultQb($account, $filters);
        $qb->addSelect('a.startDate as startDt')
            ->addGroupBy('startDt')
            ->orderBy('startDt', 'asc');

        $results = $qb->getQuery()
            ->useQueryCache(false)
            ->useResultCache(false)
            ->getResult();
        return $results;

    }

    public function skuStats($account, $filters) {

        $cacheName = md5($account.'skustats'.serialize($filters));
        $qb = $this->defaultQb($account, $filters);
        $qb->addSelect('e.sku as sku')
            ->addselect('e.sku as name')
            ->addSelect('e.img as image')
            ->addSelect('e.asin as asin')
            ->addSelect('e.url as url')
            ->addSelect('e.title as title')
            ->addSelect('e.group as category')
            ->addSelect('e.brand as brand');

        $results = $qb->getQuery()->useResultCache(false, 3600, $cacheName)
            ->getResult();

        return $results;
    }

    public function adgroupStats($account, $filters) {


        $qb = $this->defaultQb($account, $filters);
        $qb->addSelect('c.name as adgroup')
            ->addSelect('c.name as name')
            ->addSelect('c.id as adgroupId')
            ->AddGroupBy('adgroup');

        $results = $qb->getQuery()->useQueryCache(false)
            ->useResultCache(false)
            ->getResult();


        return $results;
    }


    public function campaignStats($account, $filters) {


        $qb = $this->defaultQb($account, $filters);
        $qb->addSelect('d.name as campaign')
            ->addSelect('d.name as name')
            ->addSelect('d.id as campaignId')
            ->addGroupBy('campaignId')
            ->orderBy('campaign');



        $results = $qb->getQuery()->useQueryCache(false)
            ->useResultCache(false)
            ->getResult();

        return $results;
    }



    private function defaultQb($account, $filters) {


        $qb = $this->getEntityManager()->createQueryBuilder();
        $qb->
            select('sum(a.impressions) as impressions')
            ->addSelect('sum(a.clicks) as clicks')
            ->addSelect('e.id as id')
            ->addselect('e.unitCost as baseUnitCost')
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
