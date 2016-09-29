<?php

namespace AppBundle\Entity;

use Doctrine\ORM\Mapping as ORM;

/**
 * Campaigns
 *
 * @ORM\Table(name="campaigns", indexes={@ORM\Index(name="k_name", columns={"name"})})
 * @ORM\Entity
 * @ORM\Entity(repositoryClass="CampaignsRepository") @ORM\HasLifecycleCallbacks

 */
class Campaigns extends Common
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
     * @ORM\Column(name="campaign_id", type="integer", nullable=true)
     */
    private $campaignId;

    /**
     * @var string
     *
     * @ORM\Column(name="name", type="string", length=255, nullable=false)
     */
    private $name;

    /**
     * @var string
     *
     * @ORM\Column(name="campaign_type", type="string", length=255, nullable=true)
     */
    private $campaignType;

    /**
     * @var string
     *
     * @ORM\Column(name="targeting_type", type="string", length=255, nullable=true)
     */
    private $targetingType;

    /**
     * @var float
     *
     * @ORM\Column(name="daily_budget", type="float", precision=10, scale=0, nullable=true)
     */
    private $dailyBudget = '0';

    /**
     * @var \DateTime
     *
     * @ORM\Column(name="start_date", type="date", nullable=true)
     */
    private $startDate;

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
     * @ORM\OneToMany(targetEntity="AppBundle\Entity\Adgroups", mappedBy="campaign", cascade={"remove", "persist"})
     */
    private $adGroups;

    /**
     * @var \AppBundle\Entity\Accounts
     *
     * @ORM\ManyToOne(targetEntity="AppBundle\Entity\Accounts", inversedBy="campaigns")
     * @ORM\JoinColumns({
     *   @ORM\JoinColumn(name="accounts", referencedColumnName="id")
     * })
     */
    private $accounts;

    /**
     * @param \AppBundle\Entity\Accounts $accounts
     */
    public function setAccounts($accounts)
    {
        $this->accounts = $accounts;
    }

    /**
     * @return \AppBundle\Entity\Accounts
     */
    public function getAccounts()
    {
        return $this->accounts;
    }

    /**
     * @param mixed $adGroups
     */
    public function setAdGroups($adGroups)
    {
        $this->adGroups = $adGroups;
    }

    /**
     * @return mixed
     */
    public function getAdGroups()
    {
        return $this->adGroups;
    }

    /**
     * @param int $campaignId
     * @return Campaigns
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
     * Set name
     *
     * @param string $name
     *
     * @return Campaigns
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
     * @param string $campaignType
     * @return Campaigns
     */
    public function setCampaignType($campaignType)
    {
        $this->campaignType = $campaignType;
        return $this;
    }

    /**
     * @return string
     */
    public function getCampaignType()
    {
        return $this->campaignType;
    }

    /**
     * @param string $targetingType
     * @return Campaigns
     */
    public function setTargetingType($targetingType)
    {
        $this->targetingType = $targetingType;
        return $this;
    }

    /**
     * @return string
     */
    public function getTargetingType()
    {
        return $this->targetingType;
    }

    /**
     * @param float $dailyBudget
     * @return Campaigns
     */
    public function setDailyBudget($dailyBudget)
    {
        $this->dailyBudget = $dailyBudget;
        return $this;
    }

    /**
     * @return float
     */
    public function getDailyBudget()
    {
        return $this->dailyBudget;
    }

    /**
     * @param \DateTime $startDate
     * @return Campaigns
     */
    public function setStartDate($startDate)
    {
        $this->startDate = $startDate;
        return $this;
    }

    /**
     * @return \DateTime
     */
    public function getStartDate()
    {
        return $this->startDate;
    }

    /**
     * @param string $state
     * @return Campaigns
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
     * @return Campaigns
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
     * @return Campaigns
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
     * @param \DateTime $lastUpdatedDate
     * @return Campaigns
     */
    public function setLastUpdatedDate($lastUpdatedDate)
    {
        $this->lastUpdatedDate = $lastUpdatedDate;
        return $this;
    }

    /**
     * @return \DateTime
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
     * @return Campaigns
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
     * Add adGroup
     *
     * @param \AppBundle\Entity\Adgroups $adGroup
     *
     * @return Campaigns
     */
    public function addAdGroup(\AppBundle\Entity\Adgroups $adGroup)
    {
        $this->adGroups[] = $adGroup;

        return $this;
    }

    /**
     * Remove adGroup
     *
     * @param \AppBundle\Entity\Adgroups $adGroup
     */
    public function removeAdGroup(\AppBundle\Entity\Adgroups $adGroup)
    {
        $this->adGroups->removeElement($adGroup);
    }
}
