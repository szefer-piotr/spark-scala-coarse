//Immutable lists

val captainStuff = ("Picard", "Enterprise-D", "NCC-1701-D")
println(captainStuff)

//ONE-BASED index
println(captainStuff._1)

//Key value pairs
val picardsShip = "Picard" -> "Enterprise-D"
println(picardsShip)

val aBunchOfStuff = ("Kirk", 1964, true)

// Lists
// Like a tuple but more functionality must be the same type.

val shipList = List("Enterprise", "Defiant","Voyager","Deep Space Nine")

//zero-based
shipList(0)

println(shipList.head)
println(shipList.tail)

for (ship <- shipList) {println(ship)}

// Function literal to every item in the list

// use map function .map
val backwardShips = shipList.map( (ship: String) => {ship.reverse})

for (ship <- backwardShips) {println(ship)}

//reduce() to combine together all the items in a collection using some function
val numberList = List(1,2,3,4,5)
val sum = numberList.reduce( (x: Int, y: Int) => x + y )

val iHateFives = numberList.filter( (x:Int) => x != 5)

val iHateThrees = numberList.filter(_ != 3)

// Concatenate lists

val moreNumbers = List(6,7,8)
val lotsOfNumbers = numberList ++ moreNumbers

//
val reversed = numberList.reverse
val sorted = reversed.sorted

val lotsOfDuplicates = numberList ++ numberList

val distinctValues = lotsOfDuplicates.distinct
val maxValue = numberList.max
val total = numberList.sum

//check if element egsists
val hasThree = iHateThrees.contains(3)

val shipMap = Map("Kirk" -> "Enterprise", "Picard" -> "Enterprise-D", "Sisko" -> "Deep Space Nine", "Janeway" -> "Voyager")
println(shipMap("Janeway"))
val archersShip = util.Try(shipMap("Archer")) getOrElse "Unknown"
println(archersShip)

// Create a list of the numbers 1-20; print out numbers that are evanly divisible by three.

val numbersList = (1 to 20).toList
for (numb <- numbersList) {if (numb % 3 == 0) {println(numb)}}
val onlyDivisible = numbersList.filter(_ % 3 == 0)
println(onlyDivisible)