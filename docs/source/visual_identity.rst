.. Licensed to the Apache Software Foundation (ASF) under one
.. or more contributor license agreements.  See the NOTICE file
.. distributed with this work for additional information
.. regarding copyright ownership.  The ASF licenses this file
.. to you under the Apache License, Version 2.0 (the
.. "License"); you may not use this file except in compliance
.. with the License.  You may obtain a copy of the License at
..
..   http://www.apache.org/licenses/LICENSE-2.0
..
.. Unless required by applicable law or agreed to in writing,
.. software distributed under the License is distributed on an
.. "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
.. KIND, either express or implied.  See the License for the
.. specific language governing permissions and limitations
.. under the License.

===============
Visual Identity
===============

This page presents the ADBC logo, its variants, and the geometry behind them.
The files are listed under :ref:`visual-identity-downloads`.

The Logo
========

.. container:: vi-logo vi-logo-top

   .. raw:: html
      :file: _templates/visual_identity/adbc-lockup-horizontal-currentcolor.svg

The logomark, on the left, combines two familiar pictures of data:

- the classic database icon of three stacked platters, long used for
  databases and for ODBC; and
- the triple chevrons of the `Apache Arrow logo
  <https://arrow.apache.org/visual_identity/>`_.

It is a stack of three square rings seen from above. Through the rings, their
back walls form chevrons pointing up; below them, their front walls form three
chevrons pointing down. The top ring's front walls line up exactly with the
bottom ring's back walls, so in the middle the two directions cross in an X.
Data moves both ways through ADBC: it is fetched from databases and ingested
into them.

Variants
========

.. grid:: 1 2 2 2
   :gutter: 3

   .. grid-item-card:: Horizontal lockup

      .. container:: vi-logo vi-logo-lockup

         .. raw:: html
            :file: _templates/visual_identity/adbc-lockup-horizontal-currentcolor.svg

      The main logo: the logomark with the name, set like the Apache Arrow
      wordmark: "APACHE ARROW" in Roboto Regular above "ADBC" in Barlow Bold
      at three times its cap height.

   .. grid-item-card:: Logomark

      .. container:: vi-logo vi-logo-mark

         .. raw:: html
            :file: _templates/visual_identity/adbc-logomark-currentcolor.svg

      The mark on its own, in isometric projection. Use it wherever the name
      appears nearby or isn't needed.

   .. grid-item-card:: Hex badge

      .. container:: vi-logo vi-logo-badge

         .. raw:: html
            :file: _templates/visual_identity/adbc-hex-badge-currentcolor.svg

      For hexagonal badges and stickers: the logomark in a hexagon, with
      "ADBC" and the project's URL cut out of the bottom ring's front walls.

   .. grid-item-card:: Sticker

      .. image:: _static/visual_identity/adbc-lockup-horizontal-sticker.svg
         :alt: A die-cut sticker of the horizontal lockup with a white border
         :width: 260px
         :align: center
         :class: vi-logo vi-sticker no-scaled-link

      A die-cut sticker of the horizontal lockup. The edge of the white border
      is the cut line, so upload the print file as is.

Construction
============

Every variant is built on the logomark's isometric grid, so each can be
redrawn exactly.

Logomark
--------

- **Projection:** isometric, a view from arcsin(1/√3) ≈ 35.26° above the
  horizon. Edges run at 30°, parallel to a regular hexagon's sides, and every
  angle is 60° or 120°.
- **Grid:** equilateral triangles, with lines in three directions. A cell is
  two triangles.
- **Rings:** each is 11 × 11 cells with a 9 × 9 hole, leaving a rim of 1 cell
  all round.
- **Layers:** walls are 2 cells tall and gaps 3, so the layers repeat every 5
  cells.
- **The X:** a ring is 11 cells wide, its rim plus two layer repeats
  (1 + 5 + 5), so the top layer's front walls fall on the same grid lines as
  the bottom layer's back walls. Each pair reads as one straight band, and the
  two bands cross.
- **Rhythm:** straight down the middle, rim 1, wall 2 and space 2 repeat every
  5 cells for 23 cells. Every wall, front or back, falls on this beat.
- **Notches:** each gap is a rim plus a wall (3 = 1 + 2), so each tab showing
  through the side notches is 2 × 2 cells, the same as the X's crossing.

.. card::

   .. image:: _static/visual_identity/adbc-logomark-isometric-construction-grid.svg
      :alt: Construction grid for the logomark
      :width: 100%

Horizontal lockup
-----------------

- **Grid:** the logomark's cells, with rows counted down from the mark's top
  corner. The mark's side corners sit 5½ rows down, so its side walls fall on
  half rows.
- **Type:** "ADBC" caps are 10 cells tall and "APACHE ARROW" caps 3⅓, the
  same 3:1 ratio as ARROW to APACHE in the Apache Arrow wordmark.
- **Placement:** "APACHE ARROW" sits on the top of the first layer's side
  wall. One wall below, "ADBC" runs from that wall's bottom to the bottom of
  the last layer's side wall.
- **Width:** at 3:1 both lines come out the same width, so the name is flush
  left and right with normal letter spacing.
- **Spacing:** the name sits 2 cells from the mark.

.. card::

   .. image:: _static/visual_identity/adbc-lockup-horizontal-construction-grid.svg
      :alt: Construction grid for the horizontal lockup
      :width: 100%

Hex badge
---------

- **Grid:** the logomark's own. The mark is the logomark, unchanged except
  for the type cut out of it.
- **Hexagon:** regular, and centred on the whole stack, its top face
  included. It clears the top face and the bottom walls by 3 grid lines, a
  gap, and the side walls by 3½, so its vertical sides fall on the half grid.
  Its outline is 2 lines thick, like the walls.
- **Beat:** the hexagon's lower sides sit where a fourth layer's front walls
  would be, and its upper sides mirror them, a gap above the top face.
- **Type:** cut out of the bottom layer's front walls, so the badge stays one
  color. Each line is centred on its wall and starts half a cell from the
  wall's end. "ADBC" has caps 1 cell tall, half a wall; the URL has an
  x-height of half a cell.

.. card::

   .. image:: _static/visual_identity/adbc-hex-badge-isometric-construction-grid.svg
      :alt: Construction grid for the hex badge
      :width: 100%

Usage
=====

- Use the same isometric logomark in every variant, and don't redraw it at
  another angle. Its 30° edges are what let it sit evenly in the hex badge's
  hexagon and inside the sticker's cut.
- For stickers, use the supplied die-cut file. A sticker maker's automatic
  outline decides the cut's angles and joins for itself; the approved cut runs
  parallel to the walls and places each join deliberately.

.. grid:: 1 2 2 2
   :gutter: 3

   .. grid-item-card:: Do

      .. image:: _static/visual_identity/adbc-lockup-horizontal-sticker-do.png
         :alt: The approved die-cut sticker, with notes on its edge angle and
               its joins
         :width: 100%
         :align: center

   .. grid-item-card:: Don't

      .. image:: _static/visual_identity/adbc-lockup-horizontal-sticker-dont.png
         :alt: A sticker with an automatically drawn outline, crossed out,
               with notes on its edge angle and its pinches
         :width: 100%
         :align: center

.. _visual-identity-downloads:

Downloads
=========

The logos are black: the PNGs on white, the SVGs on a transparent
background. For a white logo on a dark background, invert the PNGs, or set
the SVGs' fill to white, or to ``currentColor`` so they take the color of the
surrounding text.

.. list-table::
   :header-rows: 1

   * - Asset
     - Files
   * - Horizontal lockup
     - `SVG <_static/visual_identity/adbc-lockup-horizontal.svg>`__,
       `PNG <_static/visual_identity/adbc-lockup-horizontal.png>`__
   * - Logomark
     - `SVG <_static/visual_identity/adbc-logomark.svg>`__,
       `PNG <_static/visual_identity/adbc-logomark.png>`__
   * - Hex badge
     - `SVG <_static/visual_identity/adbc-hex-badge.svg>`__,
       `PNG <_static/visual_identity/adbc-hex-badge.png>`__
   * - Sticker print file
     - `SVG <_static/visual_identity/adbc-lockup-horizontal-sticker.svg>`__
   * - Logomark construction grid
     - `SVG <_static/visual_identity/adbc-logomark-isometric-construction-grid.svg>`__,
       `PNG <_static/visual_identity/adbc-logomark-isometric-construction-grid.png>`__
   * - Horizontal lockup construction grid
     - `SVG <_static/visual_identity/adbc-lockup-horizontal-construction-grid.svg>`__,
       `PNG <_static/visual_identity/adbc-lockup-horizontal-construction-grid.png>`__
   * - Hex badge construction grid
     - `SVG <_static/visual_identity/adbc-hex-badge-isometric-construction-grid.svg>`__,
       `PNG <_static/visual_identity/adbc-hex-badge-isometric-construction-grid.png>`__

In the logo files, all lettering is converted to shapes, so the fonts don't
need to be installed to use them. The fonts are
`Barlow <https://fonts.google.com/specimen/Barlow>`_ Bold,
`Roboto <https://fonts.google.com/specimen/Roboto>`_ Regular, and Roboto
Condensed Regular.
