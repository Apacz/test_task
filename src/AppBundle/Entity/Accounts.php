<?php

namespace AppBundle\Entity;

use Doctrine\ORM\Mapping as ORM;
use Doctrine\Common\Collections\ArrayCollection;


/**
 * Accounts
 *
 * @ORM\Table(name="accounts", uniqueConstraints={@ORM\UniqueConstraint(name="uk_username", columns={"username", "domain", "user"})},
 * indexes={@ORM\Index(name="status", columns={"status"})}
 * )
 * @ORM\Entity
 * @ORM\ChangeTrackingPolicy("DEFERRED_EXPLICIT")
 * @ORM\HasLifecycleCallbacks
 */
class Accounts
{

    public function __construct() {

        $this->orderStats = new ArrayCollection();
        $this->reports = new ArrayCollection();
        $this->campaigns = new ArrayCollection();
    }

    /** @ORM\PreUpdate */
    function onPreUpdate() {
        $this->lastDt = new \DateTime('now');
    }


    /** @ORM\PrePersist */
    function onPrePersist() {
        $this->lastDt = $this->addeddt = new \DateTime('now');
    }



    /**
     * @var string
     *
     * @ORM\Column(name="username", type="string", length=255, nullable=false)
     */
    private $username;

    /**
     * @var string
     *
     * @ORM\Column(name="seller_id", type="string", length=255, nullable=true)
     */
    private $sellerId;


    /**
     * @var string
     *
     * @ORM\Column(name="domain", type="string", length=255, nullable=false)
     */
    private $domain = 'com';

    /**
     * @var boolean
     *
     * @ORM\Column(name="active", type="boolean", nullable=false)
     */
    private $active = true;

    /**
     * @var boolean
     *
     * @ORM\Column(name="validated", type="boolean", nullable=false)
     */
    private $validated = false;

    /**
     * @var boolean
     *
     * @ORM\Column(name="needs_validation", type="boolean", nullable=false)
     */
    private $needsValidation = true;

    /**
     * @var \DateTime
     *
     * @ORM\Column(name="last_validated_dt", type="datetime", nullable=true)
     */
    private $lastValidatedDt;

    /**
     * @var boolean
     *
     * @ORM\Column(name="type", type="string", nullable=false, length=255)
     */
    private $type = 'auto';

    /**
     * @var \DateTime
     *
     * @ORM\Column(name="last_dt", type="datetime", nullable=true)
     */
    private $lastDt;

    /**
     * @var integer
     *
     * @ORM\Column(name="id", type="integer")
     * @ORM\Id
     * @ORM\GeneratedValue(strategy="IDENTITY")
     */
    private $id;

    /**
     * @var \User
     *
     * @ORM\ManyToOne(targetEntity="AppBundle\Entity\User", inversedBy="accounts")
     * @ORM\JoinColumns({
     *   @ORM\JoinColumn(name="user", referencedColumnName="id")
     * })
     */
    private $user;



    /**
     * @ORM\OneToMany(targetEntity="AppBundle\Entity\Campaigns", mappedBy="accounts", cascade={"remove", "persist"})
     **/
    private $campaigns;

    /**
     * @ORM\OneToMany(targetEntity="AppBundle\Entity\CampaignPerformance", mappedBy="accounts", cascade={"remove", "persist"})
     **/
    private $cpr;

    /**
     * @ORM\OneToMany(targetEntity="AppBundle\Entity\Sku", mappedBy="accounts")
     **/
    private $skus;

    /**
     * @var string
     *
     * @ORM\Column(name="confirm_link", type="string", length=255, nullable=true)
     */
    private $confirmLink;

    /**
     * @var string
     *
     * @ORM\Column(name="confirm_x", type="string", length=255, nullable=true)
     */
    private $confirmX;

    /**
     * @var string
     *
     * @ORM\Column(name="confirm_y", type="string", length=255, nullable=true)
     */
    private $confirmY;

    /**
     * @var string
     *
     * @ORM\Column(name="confirm_z", type="string", length=255, nullable=true)
     */
    private $confirmZ;

    /**
     * @var \DateTime
     *
     * @ORM\Column(name="authorisedDt", type="datetime", nullable=true)
     */
    private $authorisedDt;

    /**
     * @var string
     *
     * @ORM\Column(name="status", type="string", nullable=true)
     */
    private $status = 'awaiting authorisation';

    /**
     * @var boolean
     *
     * @ORM\Column(name="authorisedCode", type="string", length=255, nullable=true)
     */
    private $authorisedCode;

    /**
     * @var string
     *
     * @ORM\Column(name="auth_token", type="string", length=255, nullable=true)
     */
    private $authToken;

    /**
     * @var string
     *
     * @ORM\Column(name="auth_token_api", type="string", length=255, nullable=true)
     */
    private $authTokenApi;

    /**
     * @var string
     *
     * @ORM\Column(name="refresh_token_api", type="string", length=2048, nullable=true)

     */
    private $refreshTokenApi;

    /**
     * @var \DateTime
     *
     * @ORM\Column(name="checkDt", type="datetime", nullable=true)
     */
    private $checkDt;

    /**
     * @var boolean
     *
     * @ORM\Column(name="downloaded_historic", type="boolean", nullable=false)
     */
    private $downloadedHistoric = false;

    /**
     * @var \DateTime
     *
     * @ORM\Column(name="str_check_dt", type="datetime", nullable=true)
     */
    private $strCheckDt;

    /**
     * @var \DateTime
     *
     * @ORM\Column(name="cpr_check_dt", type="datetime", nullable=true)
     */
    private $cprCheckDt;

    /**
     * @var \DateTime
     *
     * @ORM\Column(name="sku_check_dt", type="datetime", nullable=true)
     */
    private $skuCheckDt;

    /**
     * @var \DateTime
     *
     * @ORM\Column(name="str_updated_dt", type="datetime", nullable=true)
     */
    private $strUpdatedDt;

    /**
     * @var \DateTime
     *
     * @ORM\Column(name="str_requested_dt", type="datetime", nullable=true)
     */
    private $strRequestedDt;

    /**
     * @var \DateTime
     *
     * @ORM\Column(name="cpr_updated_dt", type="datetime", nullable=true)
     */
    private $cprUpdatedDt;

    /**
     * @var \DateTime
     *
     * @ORM\Column(name="sku_updated_dt", type="datetime", nullable=true)
     */
    private $skuUpdatedDt;

    /**
     * @ORM\Column(name="addeddt", type="datetime", nullable=true)
     */
    protected $addeddt;

    /**
     * @ORM\Column(name="daily_email_dt", type="date", nullable=true)
     */
    protected $dailyEmailDt;

    /**
     * @ORM\Column(name="setup_email_dt", type="date", nullable=true)
     */
    protected $setupEmailDt;

    /* @var boolean
    *
    * @ORM\Column(name="advertising_api", type="boolean", nullable=false)
    */
    private $advertisingApi = false;

    /**
     * @var float
     *
     * @ORM\Column(name="vat", type="float", precision=10, scale=0, nullable=true)
     */
    private $vat = '0';

    /**
     * @param float $vat
     */
    public function setVat($vat)
    {
        $this->vat = $vat;
    }

    /**
     * @return float
     */
    public function getVat()
    {
        return $this->vat;
    }

    /**
     * @var string
     *
     * @ORM\Column(name="snapshot_id", type="string", length=2048, nullable=true)
     */
    private $snapshotId;

    /**
     * @var string
     *
     * @ORM\Column(name="report_id", type="string", length=2048, nullable=true)

     */
    private $reportId;

    /**
     * @param mixed $addeddt
     */
    public function setAddeddt($addeddt)
    {
        $this->addeddt = $addeddt;
    }

    /**
     * @return mixed
     */
    public function getAddeddt()
    {
        return $this->addeddt;
    }

    /**
     * @param \DateTime $cprCheckDt
     */
    public function setCprCheckDt($cprCheckDt)
    {
        $this->cprCheckDt = $cprCheckDt;
    }

    /**
     * @return \DateTime
     */
    public function getCprCheckDt()
    {
        return $this->cprCheckDt;
    }

    /**
     * @param \DateTime $skuCheckDt
     */
    public function setSkuCheckDt($skuCheckDt)
    {
        $this->skuCheckDt = $skuCheckDt;
    }

    /**
     * @return \DateTime
     */
    public function getSkuCheckDt()
    {
        return $this->skuCheckDt;
    }

    /**
     * @param \DateTime $strCheckDt
     */
    public function setStrCheckDt($strCheckDt)
    {
        $this->strCheckDt = $strCheckDt;
    }

    /**
     * @return \DateTime
     */
    public function getStrCheckDt()
    {
        return $this->strCheckDt;
    }




    /**
     * @param boolean $downloadedHistoric
     */
    public function setDownloadedHistoric($downloadedHistoric)
    {
        $this->downloadedHistoric = $downloadedHistoric;
    }

    /**
     * @return boolean
     */
    public function getDownloadedHistoric()
    {
        return $this->downloadedHistoric;
    }





    /**
     * Set username
     *
     * @param string $username
     *
     * @return Accounts
     */
    public function setUsername($username)
    {
        $this->username = $username;

        return $this;
    }

    /**
     * Get username
     *
     * @return string
     */
    public function getUsername()
    {
        return $this->username;
    }

    /**
     * Set domain
     *
     * @param string $domain
     *
     * @return Accounts
     */
    public function setDomain($domain)
    {
        $this->domain = $domain;

        return $this;
    }

    /**
     * Get domain
     *
     * @return string
     */
    public function getDomain()
    {
        return $this->domain;
    }

    /**
     * Set active
     *
     * @param boolean $active
     *
     * @return Accounts
     */
    public function setActive($active)
    {
        $this->active = $active;

        return $this;
    }

    /**
     * Get active
     *
     * @return string
     */
    public function getActive()
    {
        return $this->active;
    }

    /**
     * Set lastDt
     *
     * @param \DateTime $lastDt
     *
     * @return Accounts
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
     * Set user
     *
     * @param \User $user
     *
     * @return Accounts
     */
    public function setUser(User $user = null)
    {
        $this->user = $user;

        return $this;
    }

    /**
     * Get user
     *
     * @return \User
     */
    public function getUser()
    {
        return $this->user;
    }

    /**
     * @param mixed $orderStats
     */
    public function setOrderStats($orderStats)
    {
        $this->orderStats = $orderStats;
    }

    /**
     * @return mixed
     */
    public function getOrderStats()
    {
        return $this->orderStats;
    }

    /**
     * @param mixed $reports
     */
    public function setReports($reports)
    {
        $this->reports = $reports;
    }

    /**
     * @return mixed
     */
    public function getReports()
    {
        return $this->reports;
    }

    /**
     * @param mixed $isCurrent
     */
    public function setIsCurrent($isCurrent)
    {
        $this->isCurrent = $isCurrent;
    }

    /**
     * @return mixed
     */
    public function getIsCurrent()
    {
        return $this->isCurrent;
    }

    /**
     * @param mixed $skus
     */
    public function setSkus($skus)
    {
        $this->skus = $skus;
    }

    /**
     * @return mixed
     */
    public function getSkus()
    {
        return $this->skus;
    }

    /**
     * @param boolean $type
     */
    public function setType($type)
    {
        $this->type = $type;
    }

    /**
     * @return boolean
     */
    public function getType()
    {
        return $this->type;
    }

    /**
     * @param boolean $needsValidation
     */
    public function setNeedsValidation($needsValidation)
    {
        $this->needsValidation = $needsValidation;
    }

    /**
     * @return boolean
     */
    public function getNeedsValidation()
    {
        return $this->needsValidation;
    }

    /**
     * @param boolean $validated
     */
    public function setValidated($validated)
    {
        $this->validated = $validated;
    }

    /**
     * @return boolean
     */
    public function getValidated()
    {
        return $this->validated;
    }

    /**
     * @param \MasterAccount $masterAccount
     */
    public function setMasterAccount($masterAccount)
    {
        $this->masterAccount = $masterAccount;
    }

    /**
     * @return \MasterAccount
     */
    public function getMasterAccount()
    {
        return $this->masterAccount;
    }

    /**
     * @param boolean $authorised
     */
    public function setAuthorised($authorised)
    {
        $this->authorised = $authorised;
    }

    /**
     * @return boolean
     */
    public function getAuthorised()
    {
        return $this->authorised;
    }

    /**
     * @param boolean $authorisedCode
     */
    public function setAuthorisedCode($authorisedCode)
    {
        $this->authorisedCode = $authorisedCode;
    }

    /**
     * @return boolean
     */
    public function getAuthorisedCode()
    {
        return $this->authorisedCode;
    }

    /**
     * @param \DateTime $authorisedDt
     */
    public function setAuthorisedDt($authorisedDt)
    {
        $this->authorisedDt = $authorisedDt;
    }

    /**
     * @return \DateTime
     */
    public function getAuthorisedDt()
    {
        return $this->authorisedDt;
    }

    /**
     * @param string $confirmLink
     */
    public function setConfirmLink($confirmLink)
    {
        $this->confirmLink = $confirmLink;
    }

    /**
     * @return string
     */
    public function getConfirmLink()
    {
        return $this->confirmLink;
    }

    /**
     * @param string $confirmX
     */
    public function setConfirmX($confirmX)
    {
        $this->confirmX = $confirmX;
    }

    /**
     * @return string
     */
    public function getConfirmX()
    {
        return $this->confirmX;
    }

    /**
     * @param string $confirmY
     */
    public function setConfirmY($confirmY)
    {
        $this->confirmY = $confirmY;
    }

    /**
     * @return string
     */
    public function getConfirmY()
    {
        return $this->confirmY;
    }

    /**
     * @param string $confirmZ
     */
    public function setConfirmZ($confirmZ)
    {
        $this->confirmZ = $confirmZ;
    }

    /**
     * @return string
     */
    public function getConfirmZ()
    {
        return $this->confirmZ;
    }

    /**
     * @param \DateTime $lastValidatedDt
     */
    public function setLastValidatedDt($lastValidatedDt)
    {
        $this->lastValidatedDt = $lastValidatedDt;
    }

    /**
     * @return \DateTime
     */
    public function getLastValidatedDt()
    {
        return $this->lastValidatedDt;
    }

    /**
     * @param string $status
     */
    public function setStatus($status)
    {
        $this->status = $status;
    }

    /**
     * @return string
     */
    public function getStatus()
    {
        return $this->status;
    }

    /**
     * @param \DateTime $checkDt
     */
    public function setCheckDt($checkDt)
    {
        $this->checkDt = $checkDt;
    }

    /**
     * @return \DateTime
     */
    public function getCheckDt()
    {
        return $this->checkDt;
    }




    /**
     * Add orderStat
     *
     * @param \OrderStats $orderStat
     *
     * @return Accounts
     */
    public function addOrderStat(\OrderStats $orderStat)
    {
        $this->orderStats[] = $orderStat;

        return $this;
    }

    /**
     * Remove orderStat
     *
     * @param \OrderStats $orderStat
     */
    public function removeOrderStat(\OrderStats $orderStat)
    {
        $this->orderStats->removeElement($orderStat);
    }

    /**
     * Add report
     *
     * @param \Report $report
     *
     * @return Accounts
     */
    public function addReport(\Report $report)
    {
        $this->reports[] = $report;

        return $this;
    }

    /**
     * Remove report
     *
     * @param \Report $report
     */
    public function removeReport(\Report $report)
    {
        $this->reports->removeElement($report);
    }

    /**
     * Add skus
     *
     * @param \Sku $skus
     *
     * @return Accounts
     */
    public function addSkus(\Sku $skus)
    {
        $this->skus[] = $skus;

        return $this;
    }

    /**
     * Remove skus
     *
     * @param \Sku $skus
     */
    public function removeSkus(\Sku $skus)
    {
        $this->skus->removeElement($skus);
    }

    /**
     * @param \MasterAccount $mws
     */
    public function setMws($mws)
    {
        $this->mws = $mws;
    }

    /**
     * @return \MasterAccount
     */
    public function getMws()
    {
        return $this->mws;
    }

    /**
     * @param string $authToken
     */
    public function setAuthToken($authToken)
    {
        $this->authToken = $authToken;
    }

    /**
     * @return string
     */
    public function getAuthToken()
    {
        return $this->authToken;
    }

    /**
     * @param string $authTokenApi
     */
    public function setAuthTokenApi($authTokenApi)
    {
        $this->authTokenApi = $authTokenApi;
    }

    /**
     * @return string
     */
    public function getAuthTokenApi()
    {
        return $this->authTokenApi;
    }

    /**
     * @param string $refreshTokenApi
     */
    public function setRefreshTokenApi($refreshTokenApi)
    {
        $this->refreshTokenApi = $refreshTokenApi;
    }

    /**
     * @return string
     */
    public function getRefreshTokenApi()
    {
        return $this->refreshTokenApi;
    }

    /**
     * @param string $sellerId
     */
    public function setSellerId($sellerId)
    {
        $this->sellerId = $sellerId;
    }

    /**
     * @return string
     */
    public function getSellerId()
    {
        return $this->sellerId;
    }

    /**
     * @param mixed $cpr
     */
    public function setCpr($cpr)
    {
        $this->cpr = $cpr;
    }

    /**
     * @return mixed
     */
    public function getCpr()
    {
        return $this->cpr;
    }

    /**
     * @param mixed $campaigns
     */
    public function setCampaigns($campaigns)
    {
        $this->campaigns = $campaigns;
    }

    /**
     * @return mixed
     */
    public function getCampaigns()
    {
        return $this->campaigns;
    }

    /**
     * @param \DateTime $cprUpdatedDt
     */
    public function setCprUpdatedDt($cprUpdatedDt)
    {
        $this->cprUpdatedDt = $cprUpdatedDt;
    }

    /**
     * @return \DateTime
     */
    public function getCprUpdatedDt()
    {
        return $this->cprUpdatedDt;
    }

    /**
     * @param \DateTime $skuUpdatedDt
     */
    public function setSkuUpdatedDt($skuUpdatedDt)
    {
        $this->skuUpdatedDt = $skuUpdatedDt;
    }

    /**
     * @return \DateTime
     */
    public function getSkuUpdatedDt()
    {
        return $this->skuUpdatedDt;
    }

    /**
     * @param \DateTime $strUpdatedDt
     */
    public function setStrUpdatedDt($strUpdatedDt)
    {
        $this->strUpdatedDt = $strUpdatedDt;
    }

    /**
     * @return \DateTime
     */
    public function getStrUpdatedDt()
    {
        return $this->strUpdatedDt;
    }

    /**
     * @param \DateTime $strRequestedDt
     */
    public function setStrRequestedDt($strRequestedDt)
    {
        $this->strRequestedDt = $strRequestedDt;
    }

    /**
     * @return \DateTime
     */
    public function getStrRequestedDt()
    {
        return $this->strRequestedDt;
    }

    /**
     * @param mixed $dailyEmailDt
     */
    public function setDailyEmailDt($dailyEmailDt)
    {
        $this->dailyEmailDt = $dailyEmailDt;
    }

    /**
     * @return mixed
     */
    public function getDailyEmailDt()
    {
        return $this->dailyEmailDt;
    }



    public function accountReady() {
        if($this->type == 'auto' && (!$this->cprUpdatedDt || !$this->strUpdatedDt || !$this->skuUpdatedDt)) {
            return false;
        }
        return true;
    }

    /**
     * @param mixed $setupEmailDt
     */
    public function setSetupEmailDt($setupEmailDt)
    {
        $this->setupEmailDt = $setupEmailDt;
    }

    /**
     * @return mixed
     */
    public function getSetupEmailDt()
    {
        return $this->setupEmailDt;
    }

    public function getCurrency() {
        if($this->domain =='co.uk') {
            return '£';
        } else {
            return '$';
        }
    }


    public function getCurrencyCode() {
        if($this->domain =='co.uk') {
            return 'GBP';
        } else {
            return 'USD';
            return;
        }
    }


    /**
     * Set $advertisingApi
     *
     * @param boolean $advertisingApi
     *
     * @return Accounts
     */
    public function setAdvertisingApi($advertisingApi)
    {
        $this->advertisingApi = $advertisingApi;

        return $this;
    }

    /**
     * Get advertisingApi
     *
     * @return boolean
     */
    public function getAdvertisingApi()
    {
        return $this->advertisingApi;
    }

    /**
     * Set $snapshotId
     *
     * @param string $snapshotId
     *
     * @return Accounts
     */
    public function setSnapshotId($snapshotId)
    {
        $this->snapshotId = $snapshotId;

        return $this;
    }

    /**
     * Get snapshotId
     *
     * @return string
     */
    public function getSnapshotId()
    {
        return $this->snapshotId;
    }

    /**
     * Set $reportId
     *
     * @param string $reportId
     *
     * @return Accounts
     */
    public function setReportId($reportId)
    {
        $this->reportId = $reportId;

        return $this;
    }

    /**
     * Get reportId
     *
     * @return string
     */
    public function getReportId()
    {
        return $this->reportId;
    }

    public function getFlag() {
        $domain = $this->domain;
        $flag = $domain;
        switch ($domain) {
            case "co.uk":
                $flag = 'gb';
                break;
            case "com":
                $flag = 'us';
                break;
        }

        return $flag;
    }
}
