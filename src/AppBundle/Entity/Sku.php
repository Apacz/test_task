<?php

namespace AppBundle\Entity;

use Doctrine\ORM\Mapping as ORM;

/**
 * Sku
 *
 * @ORM\Table(name="sku")
 * @ORM\Entity
 * @ORM\Entity(repositoryClass="SkuRepository")

 */
class Sku extends Common
{
    /** @ORM\PreUpdate */
    function PreUpdate()
    {
        $this->img = str_replace('http://ecx.', 'https://images-na.ssl-', $this->img);
        $this->lastDt = new \DateTime();
    }


    /** @ORM\PrePersist */
    function PrePersist()
    {
        $this->img = str_replace('http://ecx.', 'https://images-na.ssl-', $this->img);
        $this->lastDt = new \DateTime();
    }

    /**
     * @var string
     *
     * @ORM\Column(name="sku", type="string", length=255, nullable=false)
     */
    private $sku;

    /**
     * @var string
     *
     * @ORM\Column(name="parent_asin", type="string", length=11, nullable=true)
     */
    protected $parentAsin;

    /**
     * @var string
     *
     * @ORM\Column(name="asin", type="string", length=11, nullable=true)
     */
    protected $asin;

    /**
     * @var \DateTime
     *
     * @ORM\Column(name="last_dt", type="datetime", nullable=false)
     */
    private $lastDt = 'CURRENT_TIMESTAMP';

    /**
     * @var unitCost
     *
     * @ORM\Column(name="unit_cost", type="float", precision=2, scale=0, nullable=true)
     */
    private $unitCost = '0';

    /**
     * @var shippingCost
     *
     * @ORM\Column(name="shipping_cost", type="float", precision=2, scale=0, nullable=true)
     */
    private $shippingCost = '0';

    /**
     * @var $fbaCost
     *
     * @ORM\Column(name="handling_cost", type="float", precision=2, scale=0, nullable=true)
     */
     private $handlingCost = 0;

    /**
     * @var $fbaCost
     *
     * @ORM\Column(name="pick_cost", type="float", precision=2, scale=0, nullable=true)
     */
    private $pickCost = 0;

    /**
     * @var $fbaCost
     *
     * @ORM\Column(name="weight_cost", type="float", precision=2, scale=0, nullable=true)
     */
    private $weightCost = 0;

    /**
     * @var $fbaCost
     *
     * @ORM\Column(name="referal_cost", type="float", precision=2, scale=0, nullable=true)
     */
    private $referalCost = 15;

    /**
     * @var integer
     *
     * @ORM\Column(name="id", type="integer")
     * @ORM\Id
     * @ORM\GeneratedValue(strategy="IDENTITY")
     */
    private $id;

    /**
     * @ORM\OneToMany(targetEntity="AppBundle\Entity\CampaignPerformance", mappedBy="sku" )
     */
    private $campaignPerformance;

    /**
     * @var \AppBundle\Entity\Accounts
     *
     * @ORM\ManyToOne(targetEntity="AppBundle\Entity\Accounts", inversedBy="skus")
     * @ORM\JoinColumns({
     *   @ORM\JoinColumn(name="accounts", referencedColumnName="id", nullable=false)
     * })
     */
    private $accounts;

    /**
     * @ORM\Column(name="title", type="string", length=250, nullable=true)
     */
    protected $title;

    /**
     * @ORM\Column(name="img", type="string", length=250, nullable=true)
     */
    protected $img;

    /**
     * @ORM\Column(name="group_name", type="string", length=250, nullable=true)
     */
    protected $group;

    /**
     * @ORM\Column(name="url", type="string", length=250, nullable=true)
     */
    protected $url;

    /**
     * @ORM\Column(name="min_price", type="float", nullable=true)
     */
    protected $minPrice;

    /**
     * @ORM\Column(name="max_price", type="float", nullable=true)
     */
    protected $maxPrice;

    /**
     * @ORM\Column(name="volume", type="float", nullable=true)
     */
    protected $volume;

    /**
     * @ORM\Column(name="brand", type="string", nullable=true, length=100)
     */
    protected $brand;

    /**
     * @ORM\Column(name="weight_units", type="string", nullable=true, length=100)
     */
    protected $weightUnits;

    /**
     * @ORM\Column(name="weight", type="float", nullable=true)
     */
    protected $weight;

    /**
     * @ORM\Column(name="height", type="float", nullable=true)
     */
    protected $height;

    /**
     * @ORM\Column(name="width", type="float", nullable=true)
     */
    protected $width;

    /**
     * @ORM\Column(name="length", type="float", nullable=true)
     */
    protected $length;

    /**
     * @ORM\Column(name="size_units", type="string", nullable=true, length=100)
     */
    protected $sizeUnits;

    /**
     * @ORM\Column(name="seller", type="string", nullable=true, length=3)
     */
    protected $seller;


    /**
     * @var string
     *
     * @ORM\Column(name="status", type="string", nullable=true)
     */
    protected $status = 'new';


    /**
     * @var boolean
     *
     * @ORM\Column(name="looked_up", type="boolean", nullable=true)
     */
    protected $lookedUp = false;


    /**
     * @var boolean
     *
     * @ORM\Column(name="ppc", type="boolean", nullable=false)
     */
    protected $ppc = false;


    /**
     * @ORM\ManyToMany(targetEntity="AppBundle\Entity\Adgroups", mappedBy="sku", fetch="EXTRA_LAZY")
     */
    private $adgroups;


    public function skuCosts() {
        return $this->shippingCost + $this->unitCost + $this->pickCost + $this->handlingCost + $this->weightCost;
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
     * @param mixed $campaignPerformance
     */
    public function setCampaignPerformance($campaignPerformance)
    {
        $this->campaignPerformance = $campaignPerformance;
    }

    /**
     * @return mixed
     */
    public function getCampaignPerformance()
    {
        return $this->campaignPerformance;
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
     * @param string $asin
     */
    public function setAsin($asin)
    {
        $this->asin = $asin;
    }

    /**
     * @return string
     */
    public function getAsin()
    {
        return $this->asin;
    }

    /**
     * @param string $parentAsin
     */
    public function setParentAsin($parentAsin)
    {
        $this->parentAsin = $parentAsin;
    }

    /**
     * @return string
     */
    public function getParentAsin()
    {
        return $this->parentAsin;
    }

    /**
     * @param string $sku
     */
    public function setSku($sku)
    {
        $this->sku = $sku;
    }

    /**
     * @return string
     */
    public function getSku()
    {
        return $this->sku;
    }

    /**
     * @param \AppBundle\Entity\shippingCost $fbaCost
     */
    public function setFbaCost($fbaCost)
    {
        $this->fbaCost = $fbaCost;
    }

    /**
     * @return \AppBundle\Entity\shippingCost
     */
    public function getFbaCost()
    {
        return $this->fbaCost;
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
     * @param \AppBundle\Entity\shippingCost $shippingCost
     */
    public function setShippingCost($shippingCost)
    {
        $this->shippingCost = $shippingCost;
    }

    /**
     * @return \AppBundle\Entity\shippingCost
     */
    public function getShippingCost()
    {
        return $this->shippingCost;
    }

    /**
     * @param \AppBundle\Entity\unitCost $unitCost
     */
    public function setUnitCost($unitCost)
    {
        $this->unitCost = $unitCost;
    }

    /**
     * @return \AppBundle\Entity\unitCost
     */
    public function getUnitCost()
    {
        return $this->unitCost;
    }

    /**
     * @param mixed $handlingCost
     */
    public function setHandlingCost($handlingCost)
    {
        $this->handlingCost = $handlingCost;
    }

    /**
     * @return mixed
     */
    public function getHandlingCost()
    {
        return $this->handlingCost;
    }

    /**
     * @param mixed $pickCost
     */
    public function setPickCost($pickCost)
    {
        $this->pickCost = $pickCost;
    }

    /**
     * @return mixed
     */
    public function getPickCost()
    {
        return $this->pickCost;
    }

    /**
     * @param mixed $referalCost
     */
    public function setReferalCost($referalCost)
    {
        $this->referalCost = $referalCost;
    }

    /**
     * @return mixed
     */
    public function getReferalCost()
    {
        return $this->referalCost;
    }

    /**
     * @param mixed $weightCost
     */
    public function setWeightCost($weightCost)
    {
        $this->weightCost = $weightCost;
    }

    /**
     * @return mixed
     */
    public function getWeightCost()
    {
        return $this->weightCost;
    }



    /**
     * Add campaignPerformance
     *
     * @param \AppBundle\Entity\CampaignPerformance $campaignPerformance
     *
     * @return Sku
     */
    public function addCampaignPerformance(\AppBundle\Entity\CampaignPerformance $campaignPerformance)
    {
        $this->campaignPerformance[] = $campaignPerformance;

        return $this;
    }

    /**
     * Remove campaignPerformance
     *
     * @param \AppBundle\Entity\CampaignPerformance $campaignPerformance
     */
    public function removeCampaignPerformance(\AppBundle\Entity\CampaignPerformance $campaignPerformance)
    {
        $this->campaignPerformance->removeElement($campaignPerformance);
    }

    /**
     * Add orderStat
     *
     * @param \AppBundle\Entity\OrderStats $orderStat
     *
     * @return Sku
     */
//    public function addOrderStat(\AppBundle\Entity\OrderStats $orderStat)
//    {
//        $this->orderStats[] = $orderStat;
//
//        return $this;
//    }

    /**
     * Remove orderStat
     *
     * @param \AppBundle\Entity\OrderStats $orderStat
     */
//    public function removeOrderStat(\AppBundle\Entity\OrderStats $orderStat)
//    {
//        $this->orderStats->removeElement($orderStat);
//    }

    /**
     * @param mixed $brand
     */
    public function setBrand($brand)
    {
        $this->brand = $brand;
    }

    /**
     * @return mixed
     */
    public function getBrand()
    {
        return $this->brand;
    }

    /**
     * @param mixed $group
     */
    public function setGroup($group)
    {
        $this->group = $group;
    }

    /**
     * @return mixed
     */
    public function getGroup()
    {
        return $this->group;
    }

    /**
     * @param mixed $height
     */
    public function setHeight($height)
    {
        $this->height = $height;
    }

    /**
     * @return mixed
     */
    public function getHeight()
    {
        return $this->height;
    }

    /**
     * @param mixed $img
     */
    public function setImg($img)
    {
        $this->img = $img;
        $this->img = str_replace('http://ecx.', 'https://images-na.ssl-', $this->img);
    }

    /**
     * @return mixed
     */
    public function getImg()
    {
        return $this->img;
    }

    /**
     * @param mixed $length
     */
    public function setLength($length)
    {
        $this->length = $length;
    }

    /**
     * @return mixed
     */
    public function getLength()
    {
        return $this->length;
    }

    /**
     * @param mixed $maxPrice
     */
    public function setMaxPrice($maxPrice)
    {
        $this->maxPrice = $maxPrice;
    }

    /**
     * @return mixed
     */
    public function getMaxPrice()
    {
        return $this->maxPrice;
    }

    /**
     * @param mixed $minPrice
     */
    public function setMinPrice($minPrice)
    {
        $this->minPrice = $minPrice;
    }

    /**
     * @return mixed
     */
    public function getMinPrice()
    {
        return $this->minPrice;
    }

    /**
     * @param mixed $seller
     */
    public function setSeller($seller)
    {
        $this->seller = $seller;
    }

    /**
     * @return mixed
     */
    public function getSeller()
    {
        return $this->seller;
    }

    /**
     * @param mixed $sizeUnits
     */
    public function setSizeUnits($sizeUnits)
    {
        $this->sizeUnits = $sizeUnits;
    }

    /**
     * @return mixed
     */
    public function getSizeUnits()
    {
        return $this->sizeUnits;
    }

    /**
     * @param mixed $title
     */
    public function setTitle($title)
    {
        $this->title = $title;
    }

    /**
     * @return mixed
     */
    public function getTitle()
    {
        return $this->title;
    }

    /**
     * @param mixed $url
     */
    public function setUrl($url)
    {
        $this->url = $url;
    }

    /**
     * @return mixed
     */
    public function getUrl()
    {
        return $this->url;
    }

    /**
     * @param mixed $volume
     */
    public function setVolume($volume)
    {
        $this->volume = $volume;
    }

    /**
     * @return mixed
     */
    public function getVolume()
    {
        return $this->volume;
    }

    /**
     * @param mixed $weight
     */
    public function setWeight($weight)
    {
        $this->weight = $weight;
    }

    /**
     * @return mixed
     */
    public function getWeight()
    {
        return $this->weight;
    }

    /**
     * @param mixed $weightUnits
     */
    public function setWeightUnits($weightUnits)
    {
        $this->weightUnits = $weightUnits;
    }

    /**
     * @return mixed
     */
    public function getWeightUnits()
    {
        return $this->weightUnits;
    }

    /**
     * @param mixed $width
     */
    public function setWidth($width)
    {
        $this->width = $width;
    }

    /**
     * @return mixed
     */
    public function getWidth()
    {
        return $this->width;
    }

    public function getName() {
        if($this->title) {
            return $this->title;
        } else {
            return $this->sku;
        }
    }

    /**
     * @param boolean $ppc
     */
    public function setPpc($ppc)
    {
        $this->ppc = $ppc;
    }

    /**
     * @return boolean
     */
    public function getPpc()
    {
        return $this->ppc;
    }

    /**
     * @param mixed $adgroups
     */
    public function setAdgroups($adgroups)
    {
        $this->adgroups = $adgroups;
    }

    /**
     * @return mixed
     */
    public function getAdgroups()
    {
        return $this->adgroups;
    }

    /**
     * @param boolean $lookedUp
     */
    public function setLookedUp($lookedUp)
    {
        $this->lookedUp = $lookedUp;
    }

    /**
     * @return boolean
     */
    public function getLookedUp()
    {
        return $this->lookedUp;
    }



}
