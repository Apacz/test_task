<?php

namespace AppBundle\Entity;

use Doctrine\ORM\Mapping as ORM;

/**
 * CampaignPerformance
 *
 * @ORM\Table(name="campaign_performance",
 * * indexes={
 * @ORM\Index(name="start_date", columns={"start_date", "accounts"}),
 * @ORM\Index(name="start_date_match", columns={"start_date", "accounts", "match_type"}),
 * @ORM\Index(name="keyword", columns={"keyword", "accounts"}),
 * @ORM\Index(name="account", columns={"accounts"}),
 * })
 * @ORM\Entity(repositoryClass="CampaignPerformanceRepository") @ORM\HasLifecycleCallbacks
 * @ORM\HasLifecycleCallbacks
 */
class CampaignPerformance extends Common
{
    /** @ORM\PreUpdate */
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
     * @var string
     *
     * @ORM\Column(name="keyword", type="string", length=255, nullable=false)
     */
    private $keyword;

    /**
     * @var string
     *
     * @ORM\Column(name="match_type", type="string", length=255, nullable=false)
     */
    private $matchType;

    /**
     * @var \DateTime
     *
     * @ORM\Column(name="start_date", type="date", nullable=false)
     */
    private $startDate;

    /**
     * @var \DateTime
     *
     * @ORM\Column(name="end_date", type="date", nullable=false)
     */
    private $endDate;

    /**
     * @var integer
     *
     * @ORM\Column(name="clicks", type="integer", nullable=false)
     */
    private $clicks = '0';

    /**
     * @var integer
     *
     * @ORM\Column(name="impressions", type="integer", nullable=false)
     */
    private $impressions = '0';

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
     * @var \AppBundle\Entity\Accounts
     *
     * @ORM\ManyToOne(targetEntity="AppBundle\Entity\Accounts", inversedBy="cpr")
     * @ORM\JoinColumns({
     *   @ORM\JoinColumn(name="accounts", referencedColumnName="id")
     * })
     */
    private $accounts;

    /**
     * @var \AppBundle\Entity\Adgroups
     *
     * @ORM\ManyToOne(targetEntity="AppBundle\Entity\Adgroups")
     * @ORM\JoinColumns({
     *   @ORM\JoinColumn(name="adgroup", referencedColumnName="id")
     * })
     */
    private $adgroup;

    /**
     * @var \AppBundle\Entity\Sku
     *
     * @ORM\ManyToOne(targetEntity="AppBundle\Entity\Sku", inversedBy="campaignPerformance")
     * @ORM\JoinColumns({
     *   @ORM\JoinColumn(name="sku", referencedColumnName="id")
     * })
     */
    private $sku;

    /**
     * @param \AppBundle\Entity\Accounts $accounts
     */
    public function setAccounts($accounts)
    {
        $this->accounts = $accounts;
        return $this;
    }

    /**
     * @return \AppBundle\Entity\Accounts
     */
    public function getAccounts()
    {
        return $this->accounts;
    }

    /**
     * @param \AppBundle\Entity\Adgroups $adgroup
     */
    public function setAdgroup($adgroup)
    {
        $this->adgroup = $adgroup;
        return $this;
    }

    /**
     * @return \AppBundle\Entity\Adgroups
     */
    public function getAdgroup()
    {
        return $this->adgroup;
    }

    /**
     * @param int $clicks
     */
    public function setClicks($clicks)
    {
        $this->clicks = $clicks;
        return $this;
    }

    /**
     * @return int
     */
    public function getClicks()
    {
        return $this->clicks;
    }

    /**
     * @param \DateTime $endDate
     */
    public function setEndDate($endDate)
    {
        $this->endDate = $endDate;
        return $this;
    }

    /**
     * @return \DateTime
     */
    public function getEndDate()
    {
        return $this->endDate;
    }

    /**
     * @param int $id
     */
    public function setId($id)
    {
        $this->id = $id;
        return $this;
    }

    /**
     * @return int
     */
    public function getId()
    {
        return $this->id;
    }

    /**
     * @param int $impressions
     */
    public function setImpressions($impressions)
    {
        $this->impressions = $impressions;
        return $this;
    }

    /**
     * @return int
     */
    public function getImpressions()
    {
        return $this->impressions;
    }

    /**
     * @param string $keyword
     */
    public function setKeyword($keyword)
    {
        $this->keyword = $keyword;
        return $this;
    }

    /**
     * @return string
     */
    public function getKeyword()
    {
        return $this->keyword;
    }

    /**
     * @param \DateTime $lastDt
     */
    public function setLastDt($lastDt)
    {
        $this->lastDt = $lastDt;
        return $this;
    }

    /**
     * @return \DateTime
     */
    public function getLastDt()
    {
        return $this->lastDt;
        return $this;
    }

    /**
     * @param string $matchType
     */
    public function setMatchType($matchType)
    {
        $this->matchType = $matchType;
        return $this;
    }

    /**
     * @return string
     */
    public function getMatchType()
    {
        return $this->matchType;
    }

    /**
     * @param \AppBundle\Entity\Sku $sku
     */
    public function setSku($sku)
    {
        $this->sku = $sku;
        return $this;
    }

    /**
     * @return \AppBundle\Entity\Sku
     */
    public function getSku()
    {
        return $this->sku;
    }

    /**
     * @param \DateTime $startDate
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



}
