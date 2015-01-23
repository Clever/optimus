/*
Package transforms provides a set of transformation functions that can be applied to optimus.Tables.

For backwards-compatibility, there is a Pair transform and a Join transform.

Join is the same as Pair, except that it overwrites the fields in the left row with the fields
from the right row.

In later versions, the Join transform will be removed and Pair will be renamed Join.
*/
package transforms
