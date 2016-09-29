<?php



namespace AppBundle\Entity;

use Doctrine\ORM\EntityRepository;

class CampaignsRepository extends EntityRepository
{

    public function findOrAdd($account, $name, $flush = false)
    {

        $cachename = 'campaigns-' . $name;

        $query = $this->getEntityManager()->createQuery('
                  SELECT a FROM AppBundle\Entity\Campaigns a
                  join a.accounts b
                  where b.id = :accountId
                  and a.name = :name
                ')
            ->setParameters(['accountId' => $account->getId(), 'name' => $name])
            ->setMaxResults(1)
            ->useQueryCache(true)
            ->useResultCache(true, 3600, $cachename);;
        $model = $query->getOneOrNullResult();

        if (!$model) {
            $model = new Campaigns();
            $model->setName($name);
            $model->setAccounts($account);
            $cacheDriver = $this->_em->getConfiguration()->getResultCacheImpl();
            $cacheDriver->delete($cachename);
            $this->_em->persist($model);
            if ($flush) {
                $this->_em->flush();
            }
        }

        return $model;


    }



}