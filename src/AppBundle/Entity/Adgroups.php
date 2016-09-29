<?php

namespace AppBundle\Entity;

use Doctrine\ORM\Mapping as ORM;

/**
 * Adgroups
 *
 * @ORM\Entity
 * @ORM\Entity(repositoryClass="AdgroupsRepository") @ORM\HasLifecycleCallbacks

 */
class Adgroups extends Common
{
    /** @ORM\PreUpdate  */
    function PreUpdate()
    {
        $this->lastDt = new \DateTime();
    }


    /** @ORM\PrePersist */
    function PrePersist()
    {
        $this->lastDt = new \DateTime();
    }

    /**
     * @var integer
     *
     * @ORM\Column(name="adgroup_id", type="integer", nullable=true)
     */
    private $adgroupId;

    /**
     * @var string
     *
     * @ORM\Column(name="name", type="string", length=255, nullable=false)
     */
    private $name;

    /**
     * @var integer
     *
     * @ORM\Column(name="campaign_id", type="integer", nullable=true)
     */
    private $campaignId;

    /**
     * @var float
     *
     * @ORM\Column(name="default_bid", type="float", precision=10, scale=0, nullable=true)
     */
    private $defaultBid = '0';

    /**
     * @var string
     *
     * @ORM\Column(name="state", type="string", length=255, nullable=true)
     */
    private $state;

    /**
     * @var string
     *
     * @ORM\Column(name="serving_status", type="string", length=255, nullable=true)
     */
    private $servingStatus;

    /**
     * @var \DateTime
     *
     * @ORM\Column(name="creation_date", type="date", nullable=true)
     */
    private $creationDate;

    /**
     * @var \DateTime
     *
     * @ORM\Column(name="last_updated_date", type="date", nullable=true)
     */
    private $lastUpdatedDate;

    /**
     * @var \DateTime
     *
     * @ORM\Column(name="last_dt", type="datetime", nullable=false)
     */
    private $lastDt = 'CURRENT_TIMESTAMP';

    /**
     * @var integer
     *
     * @ORM\Column(name="id", type="integer")
     * @ORM\Id
     * @ORM\GeneratedValue(strategy="IDENTITY")
     */
    private $id;

    /**
     * @var \AppBundle\Entity\Campaigns
     *
     * @ORM\ManyToOne(targetEntity="AppBundle\Entity\Campaigns", inversedBy="adGroups")
     * @ORM\JoinColumns({
     *   @ORM\JoinColumn(name="campaign", referencedColumnName="id")
     * })
     */
    private $campaign;


    /**
     * @ORM\ManyToMany(targetEntity="AppBundle\Entity\Sku", inversedBy="adgroups")
     * @ORM\JoinTable(name="sku_adgroup")
     */
    private $sku;



    public function __construct()
    {
        $this->groups = new \Doctrine\Common\Collections\ArrayCollection();
    }

    /**
     * @param mixed $negativeKeywords
     */
    public function setNegativeKeywords($negativeKeywords)
    {
        $this->negativeKeywords = $negativeKeywords;
    }

    /**
     * @return mixed
     */
    public function getNegativeKeywords()
    {
        return $this->negativeKeywords;
    }

    /**
     * @param mixed $keywords
     */
    public function setKeywords($keywords)
    {
        $this->keywords = $keywords;
    }

    /**
     * @return mixed
     */
    public function getKeywords()
    {
        return $this->keywords;
    }

    /**
     * @param mixed $sku
     */
    public function setSku($sku)
    {
        $this->sku = $sku;
    }

    /**
     * @return mixed
     */
    public function getSku()
    {
        return $this->sku;
    }

    /**
     * @param int $adgroupId
     * @return Adgroups
     */
    public function setAdgroupId($adgroupId)
    {
        $this->adgroupId = $adgroupId;
        return $this;
    }

    /**
     * @return int
     */
    public function getAdgroupId()
    {
        return $this->adgroupId;
    }

    /**
     * Set name
     *
     * @param string $name
     *
     * @return Adgroups
     */
    public function setName($name)
    {
        $this->name = $name;

        return $this;
    }

    /**
     * Get name
     *
     * @return string
     */
    public function getName()
    {
        return $this->name;
    }

    /**
     * @param int $campaignId
     * @return Adgroups
     */
    public function setCampaignId($campaignId)
    {
        $this->campaignId = $campaignId;
        return $this;
    }

    /**
     * @return int
     */
    public function getCampaignId()
    {
        return $this->campaignId;
    }

    /**
     * @param float $defaultBid
     * @return Adgroups
     */
    public function setDefaultBid($defaultBid)
    {
        $this->defaultBid = $defaultBid;
        return $this;
    }

    /**
     * @return float
     */
    public function getDefaultBid()
    {
        return $this->defaultBid;
    }

    /**
     * @param string $state
     * @return Adgroups
     */
    public function setState($state)
    {
        $this->state = $state;
        return $this;
    }

    /**
     * @return string
     */
    public function getState()
    {
        return $this->state;
    }

    /**
     * @param string $servingStatus
     * @return Adgroups
     */
    public function setServingStatus($servingStatus)
    {
        $this->servingStatus = $servingStatus;
        return $this;
    }

    /**
     * @return string
     */
    public function getServingStatus()
    {
        return $this->servingStatus;
    }

    /**
     * @param \DateTime $creationDate
     * @return Adgroups
     */
    public function setCreationDate($creationDate)
    {
        $this->creationDate = $creationDate;
        return $this;
    }

    /**
     * @return \DateTime
     */
    public function getCreationDate()
    {
        return $this->creationDate;
    }

    /**
     * @param mixed $lastUpdatedDate
     * @return Adgroups
     */
    public function setLastUpdatedDate($lastUpdatedDate)
    {
        $this->lastUpdatedDate = $lastUpdatedDate;
        return $this;
    }

    /**
     * @return mixed
     */
    public function getLastUpdatedDate()
    {
        return $this->lastUpdatedDate;
    }

    /**
     * Set lastDt
     *
     * @param \DateTime $lastDt
     *
     * @return Adgroups
     */
    public function setLastDt($lastDt)
    {
        $this->lastDt = $lastDt;

        return $this;
    }

    /**
     * Get lastDt
     *
     * @return \DateTime
     */
    public function getLastDt()
    {
        return $this->lastDt;
    }

    /**
     * Get id
     *
     * @return integer
     */
    public function getId()
    {
        return $this->id;
    }

    /**
     * Set campaign
     *
     * @param \AppBundle\Entity\Campaigns $campaign
     *
     * @return Adgroups
     */
    public function setCampaign($campaign = null)
    {
        $this->campaign = $campaign;

        return $this;
    }

    /**
     * Get campaign
     *
     * @return \AppBundle\Entity\Campaigns
     */
    public function getCampaign()
    {
        return $this->campaign;
    }

    /**
     * Add searchTerm
     *
     * @param \AppBundle\Entity\SearchTerm $searchTerm
     *
     * @return Adgroups
     */
    public function addSearchTerm(\AppBundle\Entity\SearchTerm $searchTerm)
    {
        $this->searchTerms[] = $searchTerm;

        return $this;
    }

    /**
     * Remove searchTerm
     *
     * @param \AppBundle\Entity\SearchTerm $searchTerm
     */
    public function removeSearchTerm(\AppBundle\Entity\SearchTerm $searchTerm)
    {
        $this->searchTerms->removeElement($searchTerm);
    }

    /**
     * Get searchTerms
     *
     * @return \Doctrine\Common\Collections\Collection
     */
    public function getSearchTerms()
    {
        return $this->searchTerms;
    }
}
