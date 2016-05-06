package engines

import (
	"fmt"
	sl "github.com/msackman/skiplist"
	p "goshawkdb.io/simulation"
	par "goshawkdb.io/simulation/parallel"
	"log"
	"math/rand"
	"sort"
)

/*
10301 Fatal Error (46638) at instruction <nil>
Instructions:
[Txn Received by VIC(y): Txn 2 [w(x := Eggs2) w(y := Why2)]
 Txn Received by VIB(x): Txn 2 [w(x := Eggs2) w(y := Why2)]
 Txn Votes Received by VIB(x): Txn 2 [w(x := Eggs2) w(y := Why2)]
 Txn Votes Received by VIC(y): Txn 2 [w(x := Eggs2) w(y := Why2)]
 Txn Globally Complete Received by VIC(y): Txn 2 [w(x := Eggs2) w(y := Why2)]
 Txn Globally Complete Received by VIB(x): Txn 2 [w(x := Eggs2) w(y := Why2)]
 Txn Received by VIB(x): Txn 1 [w(x := Eggs1)]
 Txn Votes Received by VIB(x): Txn 1 [w(x := Eggs1)]
 Txn Received by VIB(x): Txn 3 [w(x := Eggs3)]
 Txn Votes Received by VIB(x): Txn 3 [w(x := Eggs3)]
 Txn Globally Complete Received by VIB(x): Txn 3 [w(x := Eggs3)]
 Txn Globally Complete Received by VIB(x): Txn 1 [w(x := Eggs1)]]

            B(x)                                C(y)
             |                                   |
?2 w[x] !2a Vx1                     ?2 w[y] !2a Vy1
?2b Vx1y1 [2 +fr Vx2y2              ?2b Vx1y1 [2 +fr Vx2y2
?2d mask -> Vx2y2                   ?2d mask -> Vx2y2
?1 w[x] !1a Vx2y2                                |
?1b Vx2y2 [2,1 +fr Vx3                           |
?3 w[x] !3a Vx3                                  |
?3b Vx3 [2,1,3 +fr Vx4                           |
?3d mask -> Vx4                                  |
?1d mask nc (Vx4)                                |
             |                                   |
2015/11/11 18:17:30.738661 VIB(x) thinks last write is Txn 3 [w(x := Eggs3)] but history has Txn 1 [w(x := Eggs1)] after


DupIV, scenario 111:
12747461183 15 [
 Txn Received by VID(x): Txn 3 [w(x := Eggs3)]
 Txn Received by VIC(x): Txn 3 [w(x := Eggs3)]
 Txn Votes Received by VID(x): Txn 3 [w(x := Eggs3)]
 Txn Votes Received by VIC(x): Txn 3 [w(x := Eggs3)]
 Txn Received by VID(x): Txn 2 [w(x := Eggs2)]
 Txn Received by VIB(x): Txn 2 [w(x := Eggs2)]
 Txn Votes Received by VID(x): Txn 2 [w(x := Eggs2)]
 Txn Votes Received by VIC(x): Txn 2 [w(x := Eggs2)]
 Txn Votes Received by VIB(x): Txn 3 [w(x := Eggs3)]
 Txn Received by VID(x): Txn 1 [w(x := Eggs1)]
 Txn Received by VIB(x): Txn 1 [w(x := Eggs1)]
 Txn Votes Received by VID(x): Txn 1 [w(x := Eggs1)]
 Txn Votes Received by VIC(x): Txn 1 [w(x := Eggs1)]
 Txn Votes Received by VIB(x): Txn 1 [w(x := Eggs1)]
 Txn Votes Received by VIB(x): Txn 2 [w(x := Eggs2)]]

        Bx                     Cx                    Dx
         |            ?3 w[x] !3a Vx1        ?3 w[x] !3a Vx1
         |            ?3b Vx1 [3|            ?3b Vx1 [3
?2 w[x] !2a Vx1       ?2 w[x] !2a Vx2                 |
?3b Vx1 [3            ?2b Vx2 [3,2           ?2b Vx2 [3,2
?1 w[x] !1a Vx1                 |            ?1 w[x] !1a Vx3
?1b Vx3 [3,1          ?1b Vx3 [3,2,1         ?1b Vx2 [3,2,1
?2b Vx2 [3,2,1                  |                     |
         |                      |                     |

Special
Fatal Error (279797) at instruction <nil>
Instructions:
[Txn Received by VID(z): Txn 1 [r(x)4 w(z := Zed1)]
 Txn Received by VIC(y): Txn 2 [w(y := Why2) w(z := Zed2)]
 Txn Received by VIC(y): Txn 3 [r(x)0 w(y := Why3)]
 Txn Received by VIB(x): Txn 3 [r(x)0 w(y := Why3)]
 Txn Votes Received by VIB(x): Txn 3 [r(x)0 w(y := Why3)]
 Txn Received by VIB(x): Txn 4 [w(x := Eggs4)]
 Txn Votes Received by VIB(x): Txn 4 [w(x := Eggs4)]
 Txn Received by VIB(x): Txn 1 [r(x)4 w(z := Zed1)]
 Txn Votes Received by VID(z): Txn 1 [r(x)4 w(z := Zed1)]
 Txn Votes Received by VIB(x): Txn 1 [r(x)4 w(z := Zed1)]
 Txn Votes Received by VIC(y): Txn 3 [r(x)0 w(y := Why3)]
 Txn Received by VID(z): Txn 2 [w(y := Why2) w(z := Zed2)]
 Txn Votes Received by VID(z): Txn 2 [w(y := Why2) w(z := Zed2)]
 Txn Votes Received by VIC(y): Txn 2 [w(y := Why2) w(z := Zed2)]]

         x                      y                     z
         |                      |                     |
?3 r[x0] !3a Vx1       ?2 w[y] !2a Vy1       ?1 w[z] !1a Vz1
         |             ?3 w[y] !3a Vy1                |
?3b Vx1y1 [3           ?3b Vx1y1 [3                   |
?4 w[x] !4a Vx1y2               |                     |
?4b Vx1y2 [3,4 +fr              |                     |
?1 r[x4] !1a Vx2y2              |            ?1b Vx2y2z1 [1 +fr
?1b Vx2y2z1 [3,4,1              |            ?2 w[z] !2a Vx2y2z2
         |             ?2b Vx2y2z2 [3,2      ?2b Vx2y2z2 [1,2
[3,4,1,2

         x                      y                     z
         |                      |                     |
?3 r[x0] !3a Vx1       ?2 w[y] !2a Vy1       ?1 w[z] !1a Vz1
         |             ?3 w[y] !3a Vy1                |
?3b Vx1y1 [3           ?3b Vx1y1 [3                   |
?4 w[x] !4a Vx1y2               |                     |
?4b Vx1y2 [3,4 +fr              |                     |
?1 r[x4] !1a Vx2y2              |            ?1b Vx2y2z1 [1
?1b Vx2y2z1 [3,4,1              |            ?2 w[z] !2a Vz1
         |             ?2b Vy1z1             ?2b Vy1z1 (Vx2y1z1) [2,1
         |               (3 Vx1y1z1) [2,3             |
[2,3,4,1 both fine.

Fatal Error (95) at instruction <nil>
Instructions:
[Txn Received by VIB(x): Txn 2 [w(x := Eggs2)]
 Txn Received by VIC(y): Txn 1 [w(x := Eggs1) w(y := Why1)]
 Txn Received by VIB(x): Txn 1 [w(x := Eggs1) w(y := Why1)]
 Txn Votes Received by VIC(y): Txn 1 [w(x := Eggs1) w(y := Why1)]
 Txn Votes Received by VIB(x): Txn 1 [w(x := Eggs1) w(y := Why1)]
 Txn Votes Received by VIB(x): Txn 2 [w(x := Eggs2)]]

            B(x)                                C(y)
             |                                   |
  ?2 w[x] !2a Vx1                      ?1 w[y] !1a Vy1
  ?1 w[x] !1a Vx1                      ?1b Vx1y1 [1
  ?1b Vx1y1 [1                                   |
  ?2b Vx1 [2,1 hmm                               |

2015/10/26 17:51:22.128765 Custom:
Fatal Error (836273) at instruction <nil>
Instructions:
[Txn Received by VIC(y): Txn 4 [w(y := Why4)]
 Txn Received by VIC(y): Txn 3 [w(x := Eggs3) w(y := Why3)]
 Txn Votes Received by VIC(y): Txn 4 [w(y := Why4)]
 Txn Received by VIC(y): Txn 1 [w(x := Eggs1) w(y := Why1)]
 Txn Received by VIB(x): Txn 1 [w(x := Eggs1) w(y := Why1)]
 Txn Votes Received by VIC(y): Txn 1 [w(x := Eggs1) w(y := Why1)]
 Txn Received by VIB(x): Txn 2 [w(x := Eggs2)]
 Txn Votes Received by VIB(x): Txn 2 [w(x := Eggs2)]
 Txn Received by VIB(x): Txn 3 [w(x := Eggs3) w(y := Why3)]
 Txn Votes Received by VIC(y): Txn 3 [w(x := Eggs3) w(y := Why3)]
 Txn Votes Received by VIB(x): Txn 3 [w(x := Eggs3) w(y := Why3)]
 Txn Votes Received by VIB(x): Txn 1 [w(x := Eggs1) w(y := Why1)]]

            B(x)                                C(y)
             |                                   |
             |                       ?4 w[y] !4a Vy1 (0,4
             |                       ?3 w[y] !3a Vy1 (0,3,4
             |                       ?4b Vy1 Commit [0,4 (0,3,4 Vy2
  ?1 w[x] !1a Vx1 (0,1               ?1 w[y] !1a Vy2 (0,3,4,2
  ?2 w[x] !2a Vx1 (0,1,2             ?1b Vx1y2 Commit [0,4,1 (0,3,4,1 Vx2y3
  ?2b Vx1 Commit [0,2 (0,1,2 Vx2                 |
  ?3 w[x] !3a Vx2 (0,1,2,3                       |
  ?3b Vx2y1 Commit [0,2,3 Vx3y2      ?3b Vx2y1 Commit [0,4,3,1 Vx3y3
  ?1b Vx1y2 Commit [0,2,1,3 Vx3y3                |
             |                                   |
             |                                   |

2015/10/26 17:51:22.128765 Custom:
Fatal Error (1737713) at instruction <nil>
Instructions:
[Txn Received by VIC(y): Txn 4 [w(y := Why4)]
 Txn Received by VIC(y): Txn 3 [w(x := Eggs3) w(y := Why3)]
 Txn Votes Received by VIC(y): Txn 4 [w(y := Why4)]
 Txn Received by VIC(y): Txn 1 [w(x := Eggs1) w(y := Why1)]
 Txn Received by VIB(x): Txn 2 [w(x := Eggs2)]
 Txn Received by VIB(x): Txn 1 [w(x := Eggs1) w(y := Why1)]
 Txn Received by VIB(x): Txn 3 [w(x := Eggs3) w(y := Why3)]
 Txn Votes Received by VIC(y): Txn 1 [w(x := Eggs1) w(y := Why1)]
 Txn Votes Received by VIB(x): Txn 1 [w(x := Eggs1) w(y := Why1)]
 Txn Votes Received by VIB(x): Txn 2 [w(x := Eggs2)]
 Txn Votes Received by VIC(y): Txn 3 [w(x := Eggs3) w(y := Why3)]
 Txn Votes Received by VIB(x): Txn 3 [w(x := Eggs3) w(y := Why3)]]

            B(x)                                C(y)
             |                                   |
   ?2 w[x] !2a Vx1 (0,2                  ?4 w[y] !4a Vy1 (0,4
   ?1 w[x] !1a Vx1 (0,1,2                ?3 w[y] !3a Vy1 (0,3,4
   ?3 w[x] !3a Vx1 (0,1,2,3              ?4b Vy1 Commit [0,4 -> Vy2
             |                           ?1 w[y] !1a Vy2 (0,3,4,1
   ?1b Vx1y2 Commit [0,1 -> Vx2y3        ?1b Vx1y2 Commit [0,4,1 -> Vx2y3
   ?2b Vx1 Commit [0,1,2                         |
   ?3b Vx1y1 Commit [0,1,2,3 ARGH        ?3b Vx1y1 Commit [0,3,4,1
             |                                   |
Poop.

2015/08/05 11:40:48.109096 Scenario 10319:
        [Txn 3 [w(x := Eggs3)],
        Txn 2 [w(x := Eggs2) w(y := Why2)],
        Txn 1 [w(x := Eggs1) r(x)3 w(y := Why1)]]
Fatal Error (19270273) at instruction <nil>
Instructions:
[Txn Received by VIC(y): Txn 2 [w(x := Eggs2) w(y := Why2)]
 Txn Received by VIB(x): Txn 2 [w(x := Eggs2) w(y := Why2)]
 Txn Votes Received by VIB(x): Txn 2 [w(x := Eggs2) w(y := Why2)]
 Txn Received by VIB(x): Txn 3 [w(x := Eggs3)]
 Txn Votes Received by VIB(x): Txn 3 [w(x := Eggs3)]
 Txn Globally Complete Received by VIB(x): Txn 3 [w(x := Eggs3)]
 Txn Received by VIC(y): Txn 1 [w(x := Eggs1) r(x)3 w(y := Why1)]
 Txn Votes Received by VIC(y): Txn 2 [w(x := Eggs2) w(y := Why2)]
 Txn Received by VIB(x): Txn 1 [w(x := Eggs1) r(x)3 w(y := Why1)]
 Txn Votes Received by VIC(y): Txn 1 [w(x := Eggs1) r(x)3 w(y := Why1)]
 Txn Votes Received by VIB(x): Txn 1 [w(x := Eggs1) r(x)3 w(y := Why1)]
 Txn Globally Complete Received by VIC(y): Txn 2 [w(x := Eggs2) w(y := Why2)]
 Txn Globally Complete Received by VIB(x): Txn 2 [w(x := Eggs2) w(y := Why2)]
 Txn Globally Complete Received by VIC(y): Txn 1 [w(x := Eggs1) r(x)3 w(y := Why1)]
 Txn Globally Complete Received by VIB(x): Txn 1 [w(x := Eggs1) r(x)3 w(y := Why1)]]

            B(x)                                C(y)
             |                                   |
  ?2 w[x] !2a Vx2                     ?2 w[y] !2a Vy2
  ?2b Vx2y2 Commit [2                 ?1 w[y] !1a Vy2
  ?3 w[x] !3a Vx4y4                              |
  ?3b Vx4y4 Commit [2,4                          |
  ?3d -> Should be no change                     |
             |                                   |

Special - new
Fatal Error (419903291519) at instruction <nil>
Instructions:
[Txn Received by VID(z): Txn 1 [r(x)4 w(z := Zed1)]
 Txn Received by VID(z): Txn 2 [w(y := Why2) w(z := Zed2)]
 Txn Received by VIC(y): Txn 2 [w(y := Why2) w(z := Zed2)]
 Txn Received by VIC(y): Txn 3 [r(x)0 w(y := Why3)]
 Txn Votes Received by VID(z): Txn 2 [w(y := Why2) w(z := Zed2)]
 Txn Votes Received by VIC(y): Txn 2 [w(y := Why2) w(z := Zed2)]
 Txn Received by VIB(x): Txn 3 [r(x)0 w(y := Why3)]
 Txn Votes Received by VIC(y): Txn 3 [r(x)0 w(y := Why3)]
 Txn Votes Received by VIB(x): Txn 3 [r(x)0 w(y := Why3)]
 Txn Received by VIB(x): Txn 4 [w(x := Eggs4)]
 Txn Votes Received by VIB(x): Txn 4 [w(x := Eggs4)]
 Txn Globally Complete Received by VIB(x): Txn 4 [w(x := Eggs4)]
 Txn Received by VIB(x): Txn 1 [r(x)4 w(z := Zed1)]
 Txn Votes Received by VID(z): Txn 1 [r(x)4 w(z := Zed1)]
 Txn Votes Received by VIB(x): Txn 1 [r(x)4 w(z := Zed1)]
 Txn Globally Complete Received by VID(z): Txn 1 [r(x)4 w(z := Zed1)]
 Txn Globally Complete Received by VIB(x): Txn 1 [r(x)4 w(z := Zed1)]
 Txn Globally Complete Received by VIC(y): Txn 3 [r(x)0 w(y := Why3)]
 Txn Globally Complete Received by VIB(x): Txn 3 [r(x)0 w(y := Why3)]
 Txn Globally Complete Received by VID(z): Txn 2 [w(y := Why2) w(z := Zed2)]
 Txn Globally Complete Received by VIC(y): Txn 2 [w(y := Why2) w(z := Zed2)]]

         x                      y                     z
         |                      |                     |
  ?3 r[x0] !3a Vx0Lx0   ?2 w[y] !2a Vy0       ?1 w[z] !1a Vz0
         |              ?3 w[y] !3a Vy0       ?2 w[z] !2a Vz0
         |              ?2b Vy0z0 [2 (y1z1)   ?2b Vy0z0 [2 (y1z1)
  ?3b Vx0y0 [3 (x0y1)   ?3b Vx0y0 [2,3 (x0y1z1)       |
  ?4 w[x] !4a Vx0y1             |                     |
  ?4b Vx0y1 [3,4 (x1y1)         |                     |
  ?4d                   ?3d                           |
  ?3d -y1
  ?1 r[x4] !1a Vx4y2Lx4         |                     |
  ?1b Vx4y2z2 [3,4,1            |             ?1b Vx4y2z2 [1,2
         |                      |                     |

Fatal Error (244943291519) at instruction <nil>
Instructions:
[Txn Received by VID(z): Txn 1 [r(x)4 w(z := Zed1)]
 Txn Received by VID(z): Txn 2 [w(y := Why2) w(z := Zed2)]
 Txn Received by VIC(y): Txn 2 [w(y := Why2) w(z := Zed2)]
 Txn Received by VIC(y): Txn 3 [r(x)0 w(y := Why3)]
 Txn Votes Received by VID(z): Txn 2 [w(y := Why2) w(z := Zed2)]
 Txn Votes Received by VIC(y): Txn 2 [w(y := Why2) w(z := Zed2)]
 Txn Received by VIB(x): Txn 3 [r(x)0 w(y := Why3)]
 Txn Votes Received by VIC(y): Txn 3 [r(x)0 w(y := Why3)]
 Txn Votes Received by VIB(x): Txn 3 [r(x)0 w(y := Why3)]
 Txn Received by VIB(x): Txn 4 [w(x := Eggs4)]
 Txn Votes Received by VIB(x): Txn 4 [w(x := Eggs4)]
 Txn Globally Complete Received by VIB(x): Txn 4 [w(x := Eggs4)]
 Txn Received by VIB(x): Txn 1 [r(x)4 w(z := Zed1)]
 Txn Votes Received by VID(z): Txn 1 [r(x)4 w(z := Zed1)]
 Txn Votes Received by VIB(x): Txn 1 [r(x)4 w(z := Zed1)]
 Txn Globally Complete Received by VID(z): Txn 1 [r(x)4 w(z := Zed1)]
 Txn Globally Complete Received by VIB(x): Txn 1 [r(x)4 w(z := Zed1)]
 Txn Globally Complete Received by VID(z): Txn 2 [w(y := Why2) w(z := Zed2)]
 Txn Globally Complete Received by VIC(y): Txn 2 [w(y := Why2) w(z := Zed2)]
 Txn Globally Complete Received by VIC(y): Txn 3 [r(x)0 w(y := Why3)]
 Txn Globally Complete Received by VIB(x): Txn 3 [r(x)0 w(y := Why3)]]

         x                      y                     z
         |                      |                     |
  ?3 r[x0] !3a Vx0Lx0   ?2 w[y] !2a Vy0       ?1 w[z] !1a Vz0
         |              ?3 w[y] !3a Vy0       ?2 w[z] !2a Vz0
         |              ?2b Vy0z0 [2 (y1z1)   ?2b Vy0z0 [2 (y1z1)
  ?3b Vx0y0 [3 (x0y1)   ?3b Vx0y0 [2,3 (x0y1z1)       |
  ?4 w[x] !4a Vx0y1             |                     |
  ?4b Vx0y1 [3,4 (x1y1)         |                     |
  ?4d                   ?3d                           |
  ?3d -y1
  ?1 r[x4] !1a Vx4y2Lx4         |                     |
  ?1b Vx4y2z2 [3,4,1            |             ?1b Vx4y2z2 [1,2
         |                      |                     |

Fatal Error (39167003519) at instruction <nil>
Instructions:
[Txn Received by VID(z): Txn 1 [r(x)4 w(z := Zed1)]
 Txn Received by VID(z): Txn 2 [w(y := Why2) w(z := Zed2)]
 Txn Received by VIC(y): Txn 2 [w(y := Why2) w(z := Zed2)]
 Txn Received by VIC(y): Txn 3 [r(x)0 w(y := Why3)]
 Txn Votes Received by VID(z): Txn 2 [w(y := Why2) w(z := Zed2)]
 Txn Votes Received by VIC(y): Txn 2 [w(y := Why2) w(z := Zed2)]
 Txn Received by VIB(x): Txn 3 [r(x)0 w(y := Why3)]
 Txn Votes Received by VIC(y): Txn 3 [r(x)0 w(y := Why3)]
 Txn Votes Received by VIB(x): Txn 3 [r(x)0 w(y := Why3)]
 Txn Received by VIB(x): Txn 4 [w(x := Eggs4)]
 Txn Votes Received by VIB(x): Txn 4 [w(x := Eggs4)]
 Txn Globally Complete Received by VIB(x): Txn 4 [w(x := Eggs4)]
 Txn Globally Complete Received by VIC(y): Txn 3 [r(x)0 w(y := Why3)]
 Txn Globally Complete Received by VIB(x): Txn 3 [r(x)0 w(y := Why3)]
 Txn Received by VIB(x): Txn 1 [r(x)4 w(z := Zed1)]
 Txn Votes Received by VID(z): Txn 1 [r(x)4 w(z := Zed1)]
 Txn Votes Received by VIB(x): Txn 1 [r(x)4 w(z := Zed1)]
 Txn Globally Complete Received by VID(z): Txn 2 [w(y := Why2) w(z := Zed2)]
 Txn Globally Complete Received by VIC(y): Txn 2 [w(y := Why2) w(z := Zed2)]
 Txn Globally Complete Received by VID(z): Txn 1 [r(x)4 w(z := Zed1)]
 Txn Globally Complete Received by VIB(x): Txn 1 [r(x)4 w(z := Zed1)]]

         x                      y                     z
         |                      |                     |
  ?3 r[x0] !3a Vx0Lx0   ?2 w[y] !2a Vy0       ?1 w[z] !1a Vz0
         |              ?3 w[y] !3a Vy0       ?2 w[z] !2a Vz0
         |              ?2b Vy0z0 [2 (y1z1)   ?2b Vy0z0 [2 (y1z1)
  ?3b Vx0y0 [3 (x0y1)   ?3b Vx0y0 [2,3 (x0y1z1)       |
  ?4 w[x] !4a Vx0y1             |                     |
  ?4b Vx0y1 [3,4 (x1y1)         |                     |
  ?4d                   ?3d                           |
  ?3d -y1
  ?1 r[x4] !1a Vx4y2Lx4         |                     |
  ?1b Vx4y2z2 [3,4,1            |             ?1b Vx4y2z2 [1,2
         |                      |                     |

Fatal Error (158493611519) at instruction <nil>
Instructions:
[Txn Received by VID(z): Txn 1 [r(x)4 w(z := Zed1)]
 Txn Received by VID(z): Txn 2 [w(y := Why2) w(z := Zed2)]
 Txn Received by VIC(y): Txn 2 [w(y := Why2) w(z := Zed2)]
 Txn Received by VIC(y): Txn 3 [r(x)0 w(y := Why3)]
 Txn Votes Received by VID(z): Txn 2 [w(y := Why2) w(z := Zed2)]
 Txn Votes Received by VIC(y): Txn 2 [w(y := Why2) w(z := Zed2)]
 Txn Received by VIB(x): Txn 3 [r(x)0 w(y := Why3)]
 Txn Votes Received by VIC(y): Txn 3 [r(x)0 w(y := Why3)]
 Txn Votes Received by VIB(x): Txn 3 [r(x)0 w(y := Why3)]
 Txn Received by VIB(x): Txn 4 [w(x := Eggs4)]
 Txn Votes Received by VIB(x): Txn 4 [w(x := Eggs4)]
 Txn Globally Complete Received by VIB(x): Txn 4 [w(x := Eggs4)]
 Txn Received by VIB(x): Txn 1 [r(x)4 w(z := Zed1)]
 Txn Votes Received by VIB(x): Txn 1 [r(x)4 w(z := Zed1)]
 Txn Globally Complete Received by VIC(y): Txn 3 [r(x)0 w(y := Why3)]
 Txn Votes Received by VID(z): Txn 1 [r(x)4 w(z := Zed1)]
 Txn Globally Complete Received by VIB(x): Txn 3 [r(x)0 w(y := Why3)]
 Txn Globally Complete Received by VID(z): Txn 2 [w(y := Why2) w(z := Zed2)]
 Txn Globally Complete Received by VIC(y): Txn 2 [w(y := Why2) w(z := Zed2)]
 Txn Globally Complete Received by VID(z): Txn 1 [r(x)4 w(z := Zed1)]
 Txn Globally Complete Received by VIB(x): Txn 1 [r(x)4 w(z := Zed1)]]

         x                      y                     z
         |                      |                     |
  ?3 r[x0] !3a Vx0Lx0   ?2 w[y] !2a Vy0       ?1 w[z] !1a Vz0
         |              ?3 w[y] !3a Vy0       ?2 w[z] !2a Vz0
         |              ?2b Vy0z0 [2 (y1z1)   ?2b Vy0z0 [2 (y1z1)
  ?3b Vx0y0 [3 (x0y1)   ?3b Vx0y0 [2,3 (x0y1z1)       |
  ?4 w[x] !4a Vx0y1             |                     |
  ?4b Vx0y1 [3,4 (x1y1)         |                     |
  ?4d -y1
  ?1 r[x4] !1a Vx4y2Lx4         |                     |
  ?1b Vx4y2z2 [3,4,1            |             ?1b Vx4y2z2 [1,2
         |                      |                     |

Fatal Error (575563907519) at instruction <nil>
[Txn Received by VID(z): Txn 1 [r(x)4 w(z := Zed1)]
 Txn Received by VID(z): Txn 2 [w(y := Why2) w(z := Zed2)]
 Txn Received by VIC(y): Txn 2 [w(y := Why2) w(z := Zed2)]
 Txn Received by VIC(y): Txn 3 [r(x)0 w(y := Why3)]
 Txn Votes Received by VID(z): Txn 2 [w(y := Why2) w(z := Zed2)]
 Txn Votes Received by VIC(y): Txn 2 [w(y := Why2) w(z := Zed2)]
 Txn Received by VIB(x): Txn 3 [r(x)0 w(y := Why3)]
 Txn Votes Received by VIC(y): Txn 3 [r(x)0 w(y := Why3)]
 Txn Votes Received by VIB(x): Txn 3 [r(x)0 w(y := Why3)]
 Txn Received by VIB(x): Txn 4 [w(x := Eggs4)]
 Txn Votes Received by VIB(x): Txn 4 [w(x := Eggs4)]
 Txn Received by VIB(x): Txn 1 [r(x)4 w(z := Zed1)]
 Txn Votes Received by VID(z): Txn 1 [r(x)4 w(z := Zed1)]
 Txn Globally Complete Received by VIC(y): Txn 3 [r(x)0 w(y := Why3)]
 Txn Votes Received by VIB(x): Txn 1 [r(x)4 w(z := Zed1)]
 Txn Globally Complete Received by VIB(x): Txn 3 [r(x)0 w(y := Why3)]
 Txn Globally Complete Received by VIB(x): Txn 4 [w(x := Eggs4)]
 Txn Globally Complete Received by VID(z): Txn 1 [r(x)4 w(z := Zed1)]
 Txn Globally Complete Received by VIB(x): Txn 1 [r(x)4 w(z := Zed1)]
 Txn Globally Complete Received by VID(z): Txn 2 [w(y := Why2) w(z := Zed2)]
 Txn Globally Complete Received by VIC(y): Txn 2 [w(y := Why2) w(z := Zed2)]]

         x                      y                     z
         |                      |                     |
  ?3 r[x0] !3a Vx1Lx1   ?2 w[y] !2a Vy2       ?1 w[z] !1a Vz2
         |              ?3 w[y] !3a Vy2       ?2 w[z] !2a Vz2
         |              ?2b Vy2z2 [2          ?2b Vy2z2 [2
  ?3b Vx1y2 [3          ?3b Vx1y2 [2,3                |
  ?4 w[x] !4a Vx3y2             |                     |
  ?4b Vx3y2 [3,4                |                     |
  ?1 r[x4] !1a Vx4y2Lx4         |                     |
  ?1b Vx4y2z2 [3,4,1            |             ?1b Vx4y2z2 [1,2
         |                      |                     |

2015/07/23 14:56:50.336308 Scenario 10315:
        [Txn 3 [w(x := Eggs3)],
        Txn 2 [w(x := Eggs2) w(y := Why2)],
        Txn 1 [w(x := Eggs1) r(x)2 w(y := Why1)]]
Fatal Error (23908) at instruction <nil>
Instructions:
[Txn Received by RM[C]: Txn 2 [w(x := Eggs2) w(y := Why2)]
 Txn Received by RM[C]: Txn 1 [w(x := Eggs1) r(x)2 w(y := Why1)]
 Txn Received by RM[B]: Txn 2 [w(x := Eggs2) w(y := Why2)]
 Txn Votes Received by RM[B]: Txn 2 [w(x := Eggs2) w(y := Why2)]
 Txn Received by RM[B]: Txn 1 [w(x := Eggs1) r(x)2 w(y := Why1)]
 Txn Votes Received by RM[C]: Txn 2 [w(x := Eggs2) w(y := Why2)]
 Txn Votes Received by RM[C]: Txn 1 [w(x := Eggs1) r(x)2 w(y := Why1)]
 Txn Received by RM[B]: Txn 3 [w(x := Eggs3)]
 Txn Votes Received by RM[B]: Txn 3 [w(x := Eggs3)]
 Txn Votes Received by RM[B]: Txn 1 [w(x := Eggs1) r(x)2 w(y := Why1)]]

            B(x)                                C(y)
             |                                   |
  ?2 w[x] !2a Vx1                      ?2 w[y] !2a Vy1
  ?2b Vx2y2 Commit [2                  ?1 w[y] postpone
  ?1 r[x2]w[x] !1a Vx3y2 Lx4           ?2b Vx2y2 Commit [2
             |                                 !1a Vx2y3
  ?3 w[x] !3a Vx3y2                    ?1b Vx4y3 Lx4 Commit [2,1
  ?3b Vx4y2 Commit [2,3                          |
  ?1b Vx4y3 Lx4 Commit [2,3,1                    |
             |                                   |

2015/07/23 12:02:41.699947 Scenario 10321:
        [Txn 3 [w(x := Eggs3)],
        Txn 2 [w(x := Eggs2) w(y := Why2)],
        Txn 1 [w(x := Eggs1) r(y)0]]
Fatal Error (53620) at instruction <nil>
Instructions:
[Txn Received by RM[B]: Txn 1 [w(x := Eggs1) r(y)0]
 Txn Received by RM[C]: Txn 1 [w(x := Eggs1) r(y)0]
 Txn Received by RM[C]: Txn 2 [w(x := Eggs2) w(y := Why2)]
 Txn Received by RM[B]: Txn 2 [w(x := Eggs2) w(y := Why2)]
 Txn Votes Received by RM[C]: Txn 2 [w(x := Eggs2) w(y := Why2)]
 Txn Votes Received by RM[C]: Txn 1 [w(x := Eggs1) r(y)0]
 Txn Received by RM[B]: Txn 3 [w(x := Eggs3)]
 Txn Votes Received by RM[B]: Txn 3 [w(x := Eggs3)]
 Txn Votes Received by RM[B]: Txn 1 [w(x := Eggs1) r(y)0]
 Txn Votes Received by RM[B]: Txn 2 [w(x := Eggs2) w(y := Why2)]]

            B(x)                                C(y)
             |                                   |
  ?1 w[x] !1a Vx1                     ?1 r[y0] !1a Vy1 Ly1
  ?2 w[x] !2a Vx1                     ?2 w[y] !2a Vy1
  ?3 w[x] !3a Vx1                     ?2b Vx2y2 Commit [2
  ?3b Vx2 Commit [3                   ?1b Vx2y1 Ly1 Commit [1,2
  ?1b Vx2y1 Ly1 Commit [1,3                      |
  ?2b Vx2y2 Commit [1,2,3                        |
             |                                   |

2015/05/19 17:08:50.052574 Scenario 10648:
        [Txn 1 [r(x)3 r(y)0],
        Txn 2 [r(x)0 w(y := Why2)],
        Txn 3 [w(x := Eggs3)]]

Fatal Error (11867) at instruction <nil>
Instructions:
[Txn Received by RM[B]: Txn 2 [r(x)0 w(y := Why2)]
 Txn Received by RM[C]: Txn 1 [r(x)3 r(y)0]
 Txn Received by RM[B]: Txn 3 [w(x := Eggs3)]
 Txn Votes Received by RM[B]: Txn 3 [w(x := Eggs3)]
 Txn Received by RM[C]: Txn 2 [r(x)0 w(y := Why2)]
 Txn Votes Received by RM[C]: Txn 2 [r(x)0 w(y := Why2)]
 Txn Received by RM[B]: Txn 1 [r(x)3 r(y)0]
 Txn Votes Received by RM[C]: Txn 1 [r(x)3 r(y)0]
 Txn Votes Received by RM[B]: Txn 1 [r(x)3 r(y)0]
 Txn Votes Received by RM[B]: Txn 2 [r(x)0 w(y := Why2)]]

            B(x)                                C(y)
             |                                   |
     ?2 r[x0]; !2a Vx1 Lx1               ?1 r[y0]; !1a Vy1 Ly1
     ?3 w[x]; !3a Vx1                    ?2 w[y]; !2a Vy1
     ?3b Vx2 commit [0,3                 ?2b Vx1y2 Lx1 commit [0,2
     ?1 r[x3]; !1a Vx3 Lx3                       |
     ?1b Vx3y1 Lx3y1 commit [0,3,1       ?1b Vx3y1 Lx3y1 commit [0,1,2
     ?2b Vx1y2 Lx1 commit [0,3,2,1               |
             |                                   |
     There is no scenario in which all 3 can legally commit.
     [3,1 [2,3  [3 or [2 are you only legal options

*/

// Matthew7TxnEngine
type Matthew7TxnEngine struct {
	rng *rand.Rand
	vis []*par.VarInstance
}

func NewMatthew7TxnEngine(vis []*par.VarInstance) *Matthew7TxnEngine {
	return &Matthew7TxnEngine{
		rng: rand.New(rand.NewSource(0)),
		vis: vis,
	}
}

func (te *Matthew7TxnEngine) Clone() par.TxnEngine {
	return NewMatthew7TxnEngine(te.vis)
}

func (te *Matthew7TxnEngine) NewBallot(txn *p.Txn, voteCount, completionCount int) par.Ballot {
	return NewM7Ballot(txn, te.rng, voteCount, completionCount)
}

func (te *Matthew7TxnEngine) NewEngineVar(vi *par.VarInstance, vvv *p.VarVersionValue) par.EngineVar {
	varState := &M7VarState{
		VarVersionValue: vvv.Clone(),
		frames:          sl.New(te.rng),
		liveFrames:      sl.New(te.rng),
		rng:             te.rng,
	}
	engineVar := &Matthew7TxnEngineVar{
		VarInstance: vi,
		txns:        make(map[*p.Txn]*M7Txn),
		varState:    varState,
		rng:         te.rng,
		history:     nil,
	}
	txn0 := NewM7Txn(p.NewTxn(0), engineVar)
	txn0.historyNode = p.NewHistoryNode(nil, txn0.Txn)
	txn0.commitClock = NewVersionVector()
	txn0.commitClock[vvv.Var] = 0
	versionClock := NewVersionVector()
	versionClock[vvv.Var] = 1
	varState.curFrame = NewM7Frame(varState, txn0, versionClock)
	varState.txn0 = txn0
	txn0.frame = varState.curFrame
	return engineVar
}

func (te *Matthew7TxnEngine) NeedsCompletionNodes() bool { return false }

// Matthew7TxnEngineVar
type Matthew7TxnEngineVar struct {
	*par.VarInstance
	txns     map[*p.Txn]*M7Txn
	varState *M7VarState
	rng      *rand.Rand
	history  *p.HistoryNode
}

func (engineVar *Matthew7TxnEngineVar) TxnReceived(txn *p.Txn, ballot par.Ballot) error {
	// fmt.Printf("%v: received %v\n", engineVar, txn)
	aTxn, found := engineVar.txns[txn]
	switch {
	case found:
		return fmt.Errorf("%v: Received a txn twice? %v", engineVar, txn)
	default:
		aTxn = NewM7Txn(txn, engineVar)
		engineVar.txns[txn] = aTxn
		aTxn.Start(ballot.(*M7Ballot))
	}
	return nil
}

func (engineVar *Matthew7TxnEngineVar) TxnVotesReceived(txn *p.Txn, ballot par.Ballot) error {
	aTxn, found := engineVar.txns[txn]
	if !found {
		// we're a learner, not a voter.
		aTxn = NewM7Txn(txn, engineVar)
		engineVar.txns[txn] = aTxn
		aTxn.StartLearner(ballot.(*M7Ballot))
	}
	return aTxn.VoteReceived()
}

func (engineVar *Matthew7TxnEngineVar) TxnGloballyCompleteReceived(txn *p.Txn, ballot par.Ballot) error {
	if aTxn, found := engineVar.txns[txn]; found {
		return aTxn.GloballyCompleteReceived()
	} else {
		return fmt.Errorf("%v: Globally complete received for vanished txn: %v\n", engineVar, txn)
	}
}

func (engineVar *Matthew7TxnEngineVar) CommitHistory() *p.HistoryNode {
	if engineVar.history == nil {
		engineVar.varState.curFrame.maybeCreateChild()
		if err := engineVar.checkFinished(); err != nil {
			log.Println(err)
			return nil
		}

		engineVar.history = engineVar.calculateHistory()

		// log.Println(engineVar.history)
		// log.Println(engineVar.varState.liveFrames.Len())
		if err := engineVar.checkHistoryMatchesVarState(); err != nil {
			log.Println(err)
			return nil
		}
	}
	return engineVar.history
}

func (engineVar *Matthew7TxnEngineVar) checkFinished() error {
	if engineVar.varState.curFrame.uncommittedReads != 0 {
		return fmt.Errorf("%v: reads left over: %v", engineVar, engineVar.varState.curFrame.uncommittedReads)
	}
	if engineVar.varState.curFrame.uncommittedWrites != 0 {
		return fmt.Errorf("%v: writes left over: %v", engineVar, engineVar.varState.curFrame.uncommittedWrites)
	}
	return nil
}

func (engineVar *Matthew7TxnEngineVar) checkHistoryMatchesVarState() error {
	for _, node := range engineVar.varState.curFrame.frameTxn.historyNode.Next {
		actions := node.CommittedTxn.VarToActions[engineVar.varState.Var]
		if len(actions) != 1 || actions[0].IsWrite() {
			return fmt.Errorf("%v thinks last write is %v but history has %v after\n",
				engineVar, engineVar.varState.curFrame.frameTxn.Txn, node.CommittedTxn)
		}
	}
	return nil
}

func (engineVar *Matthew7TxnEngineVar) calculateHistory() *p.HistoryNode {
	txn0HN := engineVar.varState.txn0.historyNode
	cur, next := []*p.HistoryNode{}, []*p.HistoryNode{txn0HN}

	for node := engineVar.varState.frames.First(); node != nil; node = node.Next() {
		f := node.Key.(*M7Frame)
		cur, next = next, append([]*p.HistoryNode{}, next...)

		for childNode := f.reads.First(); childNode != nil; childNode = childNode.Next() {
			for _, parent := range cur {
				childTxnHN := childNode.Key.(*M7Txn).historyNode
				next = append(next, childTxnHN)
				parent.AddEdgeTo(childTxnHN)
			}
		}

		cur, next = next, []*p.HistoryNode{}
		if f.orderedWrites != nil {
			for childNode := f.orderedWrites.First(); childNode != nil; childNode = childNode.Next() {
				for _, parent := range cur {
					childTxnHN := childNode.Key.(*M7TxnByCommitClock).historyNode
					next = append(next, childTxnHN)
					cur = append(cur[:0], childTxnHN)
					parent.AddEdgeTo(childTxnHN)
				}
			}
		}
	}

	return txn0HN
}

// M7VersionVector
type M7VersionVector map[p.Var]int

func NewVersionVector() M7VersionVector {
	return make(map[p.Var]int)
}

func (ver M7VersionVector) Bump(v p.Var, inc int) M7VersionVector {
	ver[v] = inc + ver[v]
	return ver
}

func (verA M7VersionVector) LessThan(verB M7VersionVector) bool {
	// 1. If A has more elems than B then A cannot be < B
	if len(verA) > len(verB) {
		return false
	}
	ltFound := false
	// 2. For every elem in A, B[e] must be >= A[e]
	for k, valA := range verA {
		valB, found := verB[k]
		if !found || valB < valA {
			return false
		}
		// Have we found anything for which A[e] < B[e]?
		ltFound = ltFound || (found && valA < valB)
	}
	// 3. Everything in A is also in B and <= B. If A == B for
	// everything in A, then B must be > A if len(B) > len(A)
	return ltFound || len(verB) > len(verA)
}

// limits equality to common keys
func (verA M7VersionVector) EqualIntersect(verB M7VersionVector) bool {
	for k, valA := range verA {
		if valB, found := verB[k]; found && valA != valB {
			return false
		}
	}
	return true
}

// limits test to common keys
func (verA M7VersionVector) LessThanIntersect(verB M7VersionVector) bool {
	lessFound := false
	for k, valA := range verA {
		if valB, found := verB[k]; found {
			if valA < valB {
				lessFound = true
			} else if valA > valB {
				return false
			}
		}
	}
	return lessFound
}

func (verA M7VersionVector) MergeInFromMax(verB M7VersionVector) M7VersionVector {
	for k, v := range verB {
		verA[k] = max(verA[k], v)
	}
	return verA
}

func (verA M7VersionVector) MergeInMissing(verB M7VersionVector) M7VersionVector {
	for k, v := range verB {
		if _, found := verA[k]; !found {
			verA[k] = v
		}
	}
	return verA
}

func (ver M7VersionVector) SetVarMax(v p.Var, version int) M7VersionVector {
	ver[v] = max(version, ver[v])
	return ver
}

func (ver M7VersionVector) SubtractIfMatch(v p.Var, version int) M7VersionVector {
	if val, found := ver[v]; found && val <= version {
		delete(ver, v)
	}
	return ver
}

func (verA M7VersionVector) Clone() M7VersionVector {
	return NewVersionVector().MergeInFromMax(verA)
}

func max(a, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

func min(a, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

// M7Frame
type M7Frame struct {
	varState           *M7VarState
	frameTxn           *M7Txn
	readVoteVersion    M7VersionVector
	reads              *sl.SkipList
	maxUncommittedRead *M7Txn
	uncommittedReads   int
	completedReads     uint
	writeVoteVersion   M7VersionVector
	writes             *sl.SkipList
	orderedWrites      *sl.SkipList
	uncommittedWrites  int
	completedWrites    uint
	rwPresent          bool
	evicted            bool
	liveFramesNode     *sl.Node
	mask               M7VersionVector
}

type txnStatus int

const (
	postponed           txnStatus = iota
	uncommitted                   = iota
	committed                     = iota
	completing                    = iota
	txnGloballyComplete           = iota
)

func NewM7Frame(vs *M7VarState, txn *M7Txn, vv M7VersionVector) *M7Frame {
	f := &M7Frame{
		varState:           vs,
		frameTxn:           txn,
		readVoteVersion:    vv,
		reads:              sl.New(vs.rng),
		maxUncommittedRead: nil,
		uncommittedReads:   0,
		completedReads:     0,
		writeVoteVersion:   nil,
		writes:             sl.New(vs.rng),
		orderedWrites:      nil,
		uncommittedWrites:  0,
		completedWrites:    0,
		rwPresent:          false,
		evicted:            false,
		mask:               NewVersionVector(),
	}
	if vs.frames.Get(f) != nil {
		panic(fmt.Sprintf("Frame already exists for txn: %v!", txn.Txn))
	}
	f.liveFramesNode = vs.liveFrames.Insert(f, nil)
	vs.frames.Insert(f, nil)
	// fmt.Printf("%v New frame %v (%v:%v)\n", vs.Var, vv, txn.Txn, txn.commitClock)
	return f
}

func (a *M7Frame) Compare(bC sl.Comparable) sl.Cmp {
	if bC == nil {
		if a == nil {
			return sl.EQ
		} else {
			return sl.GT
		}
	} else {
		b := bC.(*M7Frame)
		v := a.varState.Var
		switch {
		case a == b:
			return sl.EQ
		case a == nil:
			return sl.LT
		case b == nil:
			return sl.GT
		case b.varState.Var != v:
			return sl.EQ
		case a.readVoteVersion[v] < b.readVoteVersion[v]:
			return sl.LT
		case a.readVoteVersion[v] > b.readVoteVersion[v]:
			return sl.GT
		default:
			return a.frameTxn.Compare(b.frameTxn)
		}
	}
}

func (f *M7Frame) FrameEvicted() {
	if !f.evicted {
		f.evicted = true
		f.maybeStartReadCompletions()
	}
}

func (f *M7Frame) maybeStartReadCompletions() {
	if f.evicted && f.uncommittedReads == 0 && f.reads.Len() != 0 && f.liveFramesNode.Prev() == nil {
		for rNode := f.reads.First(); rNode != nil; rNode = rNode.Next() {
			if rNode.Value == committed {
				rNode.Value = completing
				rNode.Key.(*M7Txn).LocallyCompleted()
			}
		}
	}
	f.maybeStartWriteCompletions()
}

func (f *M7Frame) maybeStartWriteCompletions() {
	if f.evicted && f.uncommittedWrites == 0 && f.writes.Len() != 0 && f.liveFramesNode.Prev() == nil {
		// fmt.Println("starting write completions")
		for wNode := f.writes.First(); wNode != nil; wNode = wNode.Next() {
			if wNode.Value == committed {
				wNode.Value = completing
				wNode.Key.(*M7Txn).LocallyCompleted()
			}
		}
	}
	f.maybeEraseFrame()
}

func (f *M7Frame) maybeEraseFrame() {
	// fmt.Printf("%v maybe erase %v (%v=%v) (%v=%v) %v\n", f.varState.txn0.engineVar, f.evicted, f.reads.Len(), f.completedReads, f.writes.Len(), f.completedWrites, f.liveFramesNode.Prev() == nil)
	if f.evicted && f.reads.Len() == f.completedReads && f.writes.Len() == f.completedWrites && f.liveFramesNode.Prev() == nil {
		// fmt.Printf("%v erasing frame\n", f.varState.txn0.engineVar)
		next := f.liveFramesNode.Next()
		f.liveFramesNode.Remove()
		nextFrame := next.Key.(*M7Frame)
		// fmt.Printf("%v Mask now at %v\n", f.varState.txn0.engineVar, f.varState.curFrame.mask)
		nextFrame.maybeStartReadCompletions()
	}
}

func (f *M7Frame) subtractClock(clock M7VersionVector) {
	for k, v := range clock {
		f.mask.SetVarMax(k, v)
		f.readVoteVersion.SubtractIfMatch(k, v)
		if f.writeVoteVersion != nil {
			f.writeVoteVersion.SubtractIfMatch(k, v)
		}
	}
}

func (f *M7Frame) AddRead(txn *M7Txn, action *p.VarAction) {
	if action.ReadVersion != f.frameTxn.ID || f.writeVoteVersion != nil ||
		(f.writes.Len() != 0 && f.writes.First().Key.Compare(txn) == sl.LT) {
		txn.Vote(nil)
		return
	}

	if node := f.reads.Get(txn); node == nil {
		f.reads.Insert(txn, uncommitted)
		f.uncommittedReads++
		if f.maxUncommittedRead == nil || f.maxUncommittedRead.Compare(txn) == sl.LT {
			f.maxUncommittedRead = txn
		}
		txn.Vote(f.readVoteVersion)
	}
}

func (f *M7Frame) ReadCommitted(txn *M7Txn) {
	if node := f.reads.Get(txn); node != nil && node.Value == uncommitted {
		node.Value = committed
		f.uncommittedReads--
		f.maybeFindMaxReadFrom(txn, node.Prev())
	}
}

func (f *M7Frame) ReadAborted(txn *M7Txn) {
	if node := f.reads.Get(txn); node != nil && node.Value == uncommitted {
		prev := node.Prev()
		node.Remove()
		f.uncommittedReads--
		f.maybeFindMaxReadFrom(txn, prev)
	}
}

func (f *M7Frame) ReadCompleted(txn *M7Txn) {
	if node := f.reads.Get(txn); node != nil && node.Value == completing {
		node.Value = txnGloballyComplete
		f.completedReads++
		f.varState.curFrame.subtractClock(txn.commitClock)
		f.maybeEraseFrame()
	}
}

func (f *M7Frame) maybeFindMaxReadFrom(txn *M7Txn, node *sl.Node) {
	if f.uncommittedReads == 0 {
		f.maxUncommittedRead = nil
		f.maybeStartWrites()
	} else if f.maxUncommittedRead.Compare(txn) == sl.EQ {
		for {
			if node.Value == uncommitted {
				f.maxUncommittedRead = node.Key.(*M7Txn)
				break
			}
			node = node.Prev()
		}
	}
}

func (f *M7Frame) maybeStartWrites() {
	if f.writes.Len() == 0 || f.uncommittedReads != 0 {
		return
	}
	f.calculateWriteVoteVersion()
	if f.uncommittedWrites == 0 {
		// learnt writes only
		f.maybeCreateChild()
	} else {
		for node := f.writes.First(); node != nil; node = node.Next() {
			if node.Value == postponed {
				node.Value = uncommitted
				node.Key.(*M7Txn).Vote(f.writeVoteVersion)
			}
		}
	}
}
func (f *M7Frame) calculateWriteVoteVersion() {
	if f.writeVoteVersion == nil {
		vv := f.readVoteVersion.Clone()
		for node := f.reads.First(); node != nil; node = node.Next() {
			txn := node.Key.(*M7Txn)
			vv.MergeInFromMax(txn.commitClock)
			for v, actions := range txn.VarToActions {
				if len(actions) == 2 || actions[0].IsWrite() {
					vv.SetVarMax(v, txn.commitClock[v]+1)
				}
			}
		}
		f.writeVoteVersion = vv
	}
}

func (f *M7Frame) AddWrite(txn *M7Txn) {
	if f.rwPresent || (f.maxUncommittedRead != nil && txn.Compare(f.maxUncommittedRead) == sl.LT) {
		txn.Vote(nil)
		return
	}

	if f.writes.Get(txn) == nil {
		f.uncommittedWrites++
		if f.uncommittedReads == 0 {
			f.writes.Insert(txn, uncommitted)
			f.calculateWriteVoteVersion()
			txn.Vote(f.writeVoteVersion)
		} else {
			f.writes.Insert(txn, postponed)
		}
	}
}

func (f *M7Frame) WriteCommitted(txn *M7Txn) {
	if node := f.writes.Get(txn); node != nil && node.Value == uncommitted {
		node.Value = committed
		f.uncommittedWrites--
		f.maybeCreateChild()
	}
}

func (f *M7Frame) WriteAborted(txn *M7Txn) {
	if node := f.writes.Get(txn); node != nil && node.Value == uncommitted {
		f.uncommittedWrites--
		node.Remove()
		if f.writes.Len() == 0 {
			f.writeVoteVersion = nil
		} else {
			f.maybeCreateChild()
		}
	}
}

func (f *M7Frame) WriteLearnt(txn *M7Txn) bool {
	if txn.commitClock[f.varState.Var] < f.readVoteVersion[f.varState.Var] {
		return false
	}
	if f.writes.Get(txn) == nil {
		f.writes.Insert(txn, committed)
		if f.uncommittedReads == 0 {
			f.calculateWriteVoteVersion()
			f.maybeCreateChild()
		}
		return true
	}
	return false
}

func (f *M7Frame) WriteCompleted(txn *M7Txn) {
	// fmt.Println("Write completed")
	if node := f.writes.Get(txn); node != nil && node.Value == completing {
		node.Value = txnGloballyComplete
		f.completedWrites++
		// fmt.Println("Write completed++", f.completedWrites)
		f.varState.curFrame.subtractClock(txn.commitClock)
		f.maybeEraseFrame()
	}
}

func (f *M7Frame) AddReadWrite(txn *M7Txn, action *p.VarAction) {
	if action.ReadVersion != f.frameTxn.ID || f.writeVoteVersion != nil ||
		f.writes.Len() != 0 || (f.maxUncommittedRead != nil && txn.Compare(f.maxUncommittedRead) == sl.LT) {
		txn.Vote(nil)
		return
	}

	f.rwPresent = true
	f.uncommittedWrites++
	f.maxUncommittedRead = txn
	if f.uncommittedReads == 0 {
		f.writes.Insert(txn, uncommitted)
		f.calculateWriteVoteVersion()
		txn.Vote(f.writeVoteVersion)
	} else {
		f.writes.Insert(txn, postponed)
	}
}

func (f *M7Frame) ReadWriteCommitted(txn *M7Txn) {
	if node := f.writes.Get(txn); node != nil && node.Value == uncommitted {
		node.Value = committed
		f.uncommittedWrites--
		f.maybeCreateChild()
	}
}

func (f *M7Frame) ReadWriteAborted(txn *M7Txn) {
	if node := f.writes.Get(txn); node != nil && node.Value == uncommitted {
		node.Remove()
		f.uncommittedWrites--
		f.rwPresent = false
		if f.writes.Len() == 0 {
			f.writeVoteVersion = nil
		} else {
			f.maybeCreateChild()
		}
	}
}

func (f *M7Frame) maybeCreateChild() *M7Frame {
	// been here before       || still working on reads  || still working on writes  || never done any writes
	if f.orderedWrites != nil || f.uncommittedReads != 0 || f.uncommittedWrites != 0 || f.writes.Len() == 0 {
		return nil
	}

	vs := f.varState
	// fmt.Printf("%v adding frame\n", vs.txn0.engineVar)

	// First we need to order by local elem. Because writes is a
	// skiplist of txns in txnid order, the lists we append to will
	// also remain in txnid order.
	localElemValToTxns := make(map[int]*[]*M7Txn)
	localElemVals := []int{}
	for node := f.writes.First(); node != nil; node = node.Next() {
		txn := node.Key.(*M7Txn)
		localElemVal := txn.commitClock[vs.Var]
		if listPtr, found := localElemValToTxns[localElemVal]; found {
			*listPtr = append(*listPtr, txn)
		} else {
			list := []*M7Txn{txn}
			localElemValToTxns[localElemVal] = &list
			localElemVals = append(localElemVals, localElemVal)
		}
	}
	sort.Ints(localElemVals)

	vv := f.writeVoteVersion.Clone()
	orderedWrites := sl.New(vs.rng)
	written := NewVersionVector()

	for _, localElemVal := range localElemVals {
		txns := localElemValToTxns[localElemVal]
		for _, txn := range *txns {
			txn.commitClock.MergeInMissing(vv)
			// fmt.Printf("%v final commit clock: %v %v\n", vs.txn0.engineVar, txn.Txn, txn.commitClock)
			orderedWrites.Insert((*M7TxnByCommitClock)(txn), nil)

			vv.MergeInFromMax(txn.commitClock)
			for v, actions := range txn.VarToActions {
				if len(actions) == 2 || actions[0].IsWrite() {
					written.SetVarMax(v, txn.commitClock[v])
				}
			}
		}
	}

	winner := (*M7Txn)(orderedWrites.Last().Key.(*M7TxnByCommitClock))
	// fmt.Printf("%v winner: %v\n", vs.txn0.engineVar, winner.Txn)
	f.orderedWrites = orderedWrites

	for k, v := range written {
		if f.mask[k] >= v {
			delete(written, k)
			// fmt.Printf("-%v ", k)
		} else {
			written[k] = v + 1
		}
	}
	for k, v := range vv {
		if f.mask[k] >= v {
			delete(vv, k)
			// fmt.Printf("-%v ", k)
		}
	}
	vv.MergeInFromMax(written)
	f1 := NewM7Frame(vs, winner, vv)
	vs.curFrame = f1
	vs.Value = winner.write.WroteValue
	vs.Version = f1.frameTxn.ID
	f.FrameEvicted()
	return f1
}

// M7VarState
type M7VarState struct {
	*p.VarVersionValue
	curFrame   *M7Frame
	frames     *sl.SkipList
	liveFrames *sl.SkipList
	rng        *rand.Rand
	txn0       *M7Txn
}

type M7TxnByCommitClock M7Txn

func (a *M7TxnByCommitClock) Compare(bC sl.Comparable) sl.Cmp {
	if bC == nil {
		if a == nil {
			return sl.EQ
		} else {
			return sl.GT
		}
	} else {
		b := bC.(*M7TxnByCommitClock)
		switch {
		case a == b:
			return sl.EQ
		case a == nil:
			return sl.LT
		case b == nil:
			return sl.GT
		default:
			alt := a.commitClock.LessThan(b.commitClock)
			blt := b.commitClock.LessThan(a.commitClock)
			switch {
			case alt == blt:
				switch {
				case a.ID < b.ID:
					return sl.LT
				case a.ID > b.ID:
					return sl.GT
				default:
					return sl.EQ
				}
			case alt:
				return sl.LT
			default:
				return sl.GT
			}
		}
	}
}

// M7Txn
type M7TxnStateMachineComponent interface {
	Init(*M7Txn)
	Start()
	m7TxnStateMachineComponentWitness()
}

type M7Txn struct {
	*p.Txn
	ballot       *M7Ballot
	learner      bool
	engineVar    *Matthew7TxnEngineVar
	varState     *M7VarState
	currentState M7TxnStateMachineComponent
	M7TxnStart
	M7TxnVote
	M7TxnReceiveVote
	M7TxnAbort
	M7TxnAwaitLocallyComplete
	M7TxnReceiveGloballyComplete
}

func NewM7Txn(txn *p.Txn, engineVar *Matthew7TxnEngineVar) *M7Txn {
	aTxn := &M7Txn{
		Txn:       txn,
		engineVar: engineVar,
		varState:  engineVar.varState,
	}
	aTxn.Init(txn)
	return aTxn
}

func (aTxn *M7Txn) Init(txn *p.Txn) {
	aTxn.M7TxnStart.Init(aTxn)
	aTxn.M7TxnVote.Init(aTxn)
	aTxn.M7TxnReceiveVote.Init(aTxn)
	aTxn.M7TxnAbort.Init(aTxn)
	aTxn.M7TxnAwaitLocallyComplete.Init(aTxn)
	aTxn.M7TxnReceiveGloballyComplete.Init(aTxn)
}

func (txn *M7Txn) GetTxnID() int { return txn.Txn.ID }

func (txn *M7Txn) Start(ballot *M7Ballot) {
	txn.ballot = ballot

	txn.currentState = &txn.M7TxnStart
	txn.currentState.Start()
}

func (txn *M7Txn) StartLearner(ballot *M7Ballot) {
	txn.ballot = ballot
	txn.learner = true

	txn.currentState = &txn.M7TxnReceiveVote
	txn.currentState.Start()
}

func (txn *M7Txn) VoteReceived() error {
	if txn.currentState == &txn.M7TxnReceiveVote {
		txn.voteReceived()
		return nil
	} else if !txn.ballot.IsAbort() {
		// If we're not in that state then the outcome must be abort
		// because we haven't voted yet!
		return fmt.Errorf("Non-abort outcome received without us voting! %v",
			txn.Txn)
	} else {
		return nil
	}
}

func (txn *M7Txn) GloballyCompleteReceived() error {
	if txn.currentState == &txn.M7TxnReceiveGloballyComplete {
		return txn.globallyCompleteReceived()
	} else {
		return fmt.Errorf("Globally complete received without us being ready! %v %v", txn.Txn, txn.currentState)
	}
}

func (txn *M7Txn) nextState(requestedState M7TxnStateMachineComponent) {
	if requestedState == nil {
		switch txn.currentState {
		case &txn.M7TxnStart:
			txn.currentState = &txn.M7TxnVote
		case &txn.M7TxnVote:
			txn.currentState = &txn.M7TxnReceiveVote
		case &txn.M7TxnReceiveVote:
			txn.currentState = &txn.M7TxnAwaitLocallyComplete
		case &txn.M7TxnAbort:
			txn.currentState = &txn.M7TxnAwaitLocallyComplete
		case &txn.M7TxnAwaitLocallyComplete:
			txn.currentState = &txn.M7TxnReceiveGloballyComplete
		}
		txn.currentState.Start()

	} else {
		txn.currentState = requestedState
		txn.currentState.Start()
	}
}

// M7TxnStart
type M7TxnStart struct {
	*M7Txn
	read          *p.VarAction
	write         *p.VarAction
	isRead        bool
	isWrite       bool
	remoteActions map[p.Var]p.VarActions
	pendingStarts int
}

func (start *M7TxnStart) m7TxnStateMachineComponentWitness() {}

func (start *M7TxnStart) String() string { return "M7TxnStart" }

func (start *M7TxnStart) Init(txn *M7Txn) {
	start.M7Txn = txn
	start.remoteActions = make(map[p.Var]p.VarActions)

	localActionCount := 0
	for v, actions := range start.VarToActions {
		if v == start.engineVar.Var {
			for _, action := range actions {
				localActionCount++
				if action.IsRead() {
					start.read = action
					start.isRead = true
				} else {
					start.write = action
					start.isWrite = true
				}
			}
		} else {
			start.remoteActions[v] = actions
		}
	}
	start.pendingStarts = localActionCount
}

func (start *M7TxnStart) Start() {
	// fmt.Printf("%v: %v started\n", start.engineVar, start.Txn)
	vs := start.varState

	switch {
	case start.isRead && start.isWrite:
		vs.curFrame.AddReadWrite(start.M7Txn, start.read)
	case start.isRead:
		vs.curFrame.AddRead(start.M7Txn, start.read)
	default:
		vs.curFrame.AddWrite(start.M7Txn)
	}
}

func (start *M7TxnStart) Vote(clock M7VersionVector) {
	if start.currentState == start {
		start.commitClock = clock
		start.nextState(nil)
	}
}

// M7TxnVote
type M7TxnVote struct {
	*M7Txn
	voteAbort   bool
	frame       *M7Frame
	commitClock M7VersionVector
}

func (vote *M7TxnVote) m7TxnStateMachineComponentWitness() {}

func (vote *M7TxnVote) String() string { return "M7TxnVote" }

func (vote *M7TxnVote) Init(txn *M7Txn) {
	vote.M7Txn = txn
	vote.voteAbort = false
}

func (vote *M7TxnVote) Start() {
	// fmt.Printf("%v: voting on %v\n", vote.engineVar, vote.Txn)
	ballot := vote.ballot

	// We now need to check our reads are legal again: even though we
	// check them as reads get started, it could be that some
	// previously legal reads have now become illegal.
	vote.voteAbort = vote.commitClock == nil

	if vote.voteAbort {
		ballot.Abort()
		ballot.VoteCast()
		vote.nextState(nil)
		return
	}

	// fmt.Printf("%v: voting on %v with version %v\n", vote.engineVar, vote.Txn, version)
	ballot.SetVersion(vote.commitClock)
	ballot.VoteCast()

	vote.frame = vote.varState.curFrame
	vote.nextState(nil)
}

// M7TxnReceiveVote
type M7TxnReceiveVote struct {
	*M7Txn
	historyNode *p.HistoryNode
}

func (rv *M7TxnReceiveVote) m7TxnStateMachineComponentWitness() {}

func (rv *M7TxnReceiveVote) String() string { return "M7TxnReceiveVote" }

func (rv *M7TxnReceiveVote) Init(txn *M7Txn) {
	rv.M7Txn = txn
}

func (rv *M7TxnReceiveVote) Start() {}

func (rv *M7TxnReceiveVote) voteReceived() {
	vs := rv.varState
	ballot := rv.ballot

	// fmt.Printf("%v: received vote for %v. Aborted? %v\n", rv.engineVar, rv.Txn, ballot.IsAbort())
	if ballot.IsAbort() {
		rv.nextState(&rv.M7TxnAbort)
		return
	}

	rv.commitClock = ballot.CombinedVersion()
	rv.historyNode = p.NewHistoryNode(nil, rv.Txn)
	// fmt.Printf("%v: received vote for %v. commitClock %v\n", rv.engineVar, rv.Txn, rv.commitClock)

	if rv.learner { // learner
		if rv.isWrite {
			curFrame := vs.curFrame
			if curFrame.WriteLearnt(rv.M7Txn) {
				rv.frame = vs.curFrame
			} else {
				rv.LocallyCompleted()
			}
		} else {
			rv.LocallyCompleted()
		}

	} else {
		switch {
		case rv.isRead && rv.isWrite:
			rv.frame.ReadWriteCommitted(rv.M7Txn)
		case rv.isRead:
			rv.frame.ReadCommitted(rv.M7Txn)
		default:
			rv.frame.WriteCommitted(rv.M7Txn)
		}
	}

	rv.nextState(nil)
}

// M7TxnAbort
type M7TxnAbort struct {
	*M7Txn
	aborted bool
}

func (abort *M7TxnAbort) m7TxnStateMachineComponentWitness() {}

func (abort *M7TxnAbort) String() string { return "M7TxnAbort" }

func (abort *M7TxnAbort) Init(txn *M7Txn) {
	abort.M7Txn = txn
}

func (abort *M7TxnAbort) Start() {
	// fmt.Printf("%v: %v aborted\n", abort.engineVar, abort.Txn)
	abort.aborted = true

	if abort.frame != nil {
		switch {
		case abort.isRead && abort.isWrite:
			abort.frame.ReadWriteAborted(abort.M7Txn)
		case abort.isRead:
			abort.frame.ReadAborted(abort.M7Txn)
		default:
			abort.frame.WriteAborted(abort.M7Txn)
		}
	}

	abort.nextState(nil)
}

// M7TxnAwaitLocallyComplete
type M7TxnAwaitLocallyComplete struct {
	*M7Txn
	locallyComplete bool
}

func (acf *M7TxnAwaitLocallyComplete) m7TxnStateMachineComponentWitness() {}

func (acf *M7TxnAwaitLocallyComplete) String() string { return "M7TxnAwaitLocallyComplete" }

func (acf *M7TxnAwaitLocallyComplete) Init(txn *M7Txn) {
	acf.M7Txn = txn
}

func (acf *M7TxnAwaitLocallyComplete) Start() {
	if acf.aborted {
		acf.locallyComplete = true
	}
	if acf.locallyComplete {
		acf.nextState(nil)
	}
}

func (acf *M7TxnAwaitLocallyComplete) LocallyCompleted() {
	if !acf.locallyComplete && !acf.aborted {
		acf.locallyComplete = true
		if acf.currentState == acf {
			acf.nextState(nil)
		}
	}
}

// M7TxnReceiveGloballyComplete
type M7TxnReceiveGloballyComplete struct {
	*M7Txn
}

func (rgc *M7TxnReceiveGloballyComplete) m7TxnStateMachineComponentWitness() {}

func (rgc *M7TxnReceiveGloballyComplete) String() string { return "M7TxnReceiveGloballyComplete" }

func (rgc *M7TxnReceiveGloballyComplete) Init(txn *M7Txn) {
	rgc.M7Txn = txn
}

func (rgc *M7TxnReceiveGloballyComplete) Start() {
	// fmt.Printf("%v: awaiting globally completed - start: %v\n", rgc.engineVar, rgc.Txn)
	rgc.ballot.TxnLocallyComplete()
}

func (rgc *M7TxnReceiveGloballyComplete) globallyCompleteReceived() error {
	// fmt.Printf("%v: global completion received for %v\n", rgc.engineVar, rgc.Txn)
	delete(rgc.engineVar.txns, rgc.Txn)
	if rgc.aborted {
		return nil
	}

	// fmt.Printf("%v: frame complete for %v, deleting %v from versionClock %v <= %v\n", rgc.engineVar, rgc.Txn, v, curVsn, vsn)

	if rgc.frame != nil {
		if rgc.isWrite {
			rgc.frame.WriteCompleted(rgc.M7Txn)
		} else {
			rgc.frame.ReadCompleted(rgc.M7Txn)
		}
	}

	return nil
}

// M7Ballot
type M7Ballot struct {
	*p.Txn
	votesRequired      int
	votesRemaining     int
	completesRemaining int
	aborted            bool
	combinedVersion    M7VersionVector
}

func NewM7Ballot(txn *p.Txn, rng *rand.Rand, votesRequired, completesRequired int) *M7Ballot {
	return &M7Ballot{
		Txn:                txn,
		votesRequired:      votesRequired,
		votesRemaining:     votesRequired,
		completesRemaining: completesRequired,
		aborted:            false,
		combinedVersion:    NewVersionVector(),
	}
}

func (ballot *M7Ballot) Abort() {
	ballot.aborted = true
}

func (ballot *M7Ballot) IsAbort() bool {
	return ballot.aborted
}

func (ballot *M7Ballot) IsStable() bool {
	return ballot.aborted || ballot.votesRemaining == 0
}

func (ballot *M7Ballot) VoteCast() {
	ballot.votesRemaining--
}

func (ballot *M7Ballot) AllVotesReceived() bool {
	return ballot.votesRemaining == 0
}

func (ballot *M7Ballot) TxnLocallyComplete() {
	ballot.completesRemaining--
}

func (ballot *M7Ballot) AllLocallyComplete() bool {
	return ballot.completesRemaining == 0
}

func (ballot *M7Ballot) SetVersion(ver M7VersionVector) {
	ballot.combinedVersion.MergeInFromMax(ver)
}

func (ballot *M7Ballot) CombinedVersion() M7VersionVector {
	return ballot.combinedVersion
}
