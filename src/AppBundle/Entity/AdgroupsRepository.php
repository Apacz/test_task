<?php


namespace AppBundle\Entity;
use Doctrine\ORM\Mapping as ORM;
use Doctrine\ORM\EntityRepository;

class AdgroupsRepository  extends EntityRepository {

    public function findOrAdd($campaign, $name ,$flush = false) {

        $cachename = 'adgroup-'.$name;
        $query = $this->getEntityManager()->createQuery('
                  SELECT a FROM AppBundle\Entity\Adgroups a
                  join a.campaign b
                  where b.id = :campaignId
                  and a.name = :name
                ')
            ->setParameters(['campaignId' => $campaign->getId(), 'name' => $name] )
            ->setMaxResults(1)
            ->useQueryCache(false)
            ->useResultCache(false, 3600, $cachename);
        ;
        $model = $query->getOneOrNullResult();
        if(!$model) {
            $model = new Adgroups();
            $model->setName($name);
            $model->setCampaign($campaign);
            $this->_em->persist($model);
            $cacheDriver = $this->_em->getConfiguration()->getResultCacheImpl();
            $cacheDriver->delete($cachename);
            $cacheDriver->deleteAll();
            if($flush) {
                $this->_em->flush();
            }
        }
        return $model;


    }


}