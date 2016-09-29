<?php


namespace AppBundle\Entity;

use Doctrine\ORM\Mapping as ORM;
use Doctrine\ORM\EntityRepository;

class SkuRepository  extends EntityRepository {


    public function findOrAdd($account, $sku, $data = [], $flush = false) {
        $cachename = 'sku-'.$sku.$account->getId();
        $query = $this->getEntityManager()->createQuery('
                  SELECT a FROM AppBundle\Entity\Sku a
                  join a.accounts b
                  where b.id = :accountId
                  and a.sku = :sku
                ')
            ->setParameters(['accountId' => $account->getId(), 'sku' => $sku] )
            ->setMaxResults(1)
            ->useQueryCache(false)
            ->useResultCache(false, 3600, $cachename);
        ;;
        $model = $query->getOneOrNullResult();


        if(!$model) {
            $model = new Sku();
            $model->setSku($sku);
            $model->setAccounts($account);
            $model->setOptions($data);
            $this->_em->persist($model);
            $cacheDriver = $this->_em->getConfiguration()->getResultCacheImpl();
            $cacheDriver->delete($cachename);
            if($flush) {
                $this->_em->flush();
            }
        } else {
            $model->setOptions($data);
        }


        return $model;

    }


}