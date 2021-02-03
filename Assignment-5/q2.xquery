for $d in  distinct-values( doc("purchaseorders.xml")//item/partid)
let $items:= doc("purchaseorders.xml")//item[partid= $d]/quantity 
let $price:= distinct-values( doc("purchaseorders.xml")//item[partid= $d]/price )
order by $d
return <totalCost partid ="{$d}"> {sum($items)*$price} </totalCost>