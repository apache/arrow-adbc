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
back walls form three chevrons pointing up; below them, their front walls form
three chevrons pointing down. Data moves both ways through ADBC: it is fetched
from databases and ingested into them.

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

      The mark on its own, in 3:4 dimetric projection. Use it wherever the
      name appears nearby or isn't needed.

   .. grid-item-card:: Hex badge

      .. container:: vi-logo vi-logo-badge

         .. raw:: html
            :file: _templates/visual_identity/adbc-hex-badge-currentcolor.svg

      For hexagonal badges and stickers. The mark is redrawn in isometric
      projection to fit the hexagon.

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

Every edge lies on a grid derived from the logomark, so each variant can be
redrawn exactly.

Logomark
--------

- **Projection:** 3:4 dimetric. Edges rise 3 for every 4 across, a 3-4-5
  triangle, for a view from arcsin(3/4) ≈ 48.59° above the horizon.
- **Rings:** each is 10 × 10 cells with an 8 × 8 hole, leaving a rim of 1
  cell all round.
- **Layers:** walls are 1 cell tall and gaps 2, so the layers repeat every 3
  cells.
- **Weight:** rim and wall are both 1 cell, so every band, black or white, is
  equally thick.
- **Rhythm:** straight down the middle, rim, wall and space repeat 1:1:1 for
  17 cells.
- **Notches:** gaps are twice the rim, so each tab showing through the side
  notches is exactly 1 cell.

.. card::

   .. image:: _static/visual_identity/adbc-logomark-dimetric-construction-grid.svg
      :alt: Construction grid for the logomark
      :width: 100%

Horizontal lockup
-----------------

- **Grid:** the logomark's cells, with rows counted down from the mark's top
  corner.
- **Type:** "ADBC" caps are 6 cells tall and "APACHE ARROW" caps 2, the same
  3:1 ratio as ARROW to APACHE in the Apache Arrow wordmark.
- **Placement:** "APACHE ARROW" sits on the top of the first layer's side
  wall. One cell below, "ADBC" runs from that wall's bottom to the bottom of
  the last layer's wall, centred on the mark as ARROW is on the Arrow
  chevrons.
- **Width:** at 3:1 both lines come out the same width, so the name is flush
  left and right with normal letter spacing.
- **Spacing:** the name sits 2 cells from the mark.

.. card::

   .. image:: _static/visual_identity/adbc-lockup-horizontal-construction-grid.svg
      :alt: Construction grid for the horizontal lockup
      :width: 100%

Hex badge
---------

- **Projection:** isometric, a view from arcsin(1/√3) ≈ 35.26° above the
  horizon. Edges run at 30°, parallel to a regular hexagon's sides, and every
  angle is 60° or 120°.
- **Grid:** equilateral triangles, with lines in three directions. The mark
  keeps the logomark's cell counts.
- **Hexagon:** centred on the mark, with every side on a grid line: 3 lines
  clear at the sides and 5 at the top and bottom. Its outline is 1 line thick,
  like the walls.
- **Type:** set on the half grid, hanging half a cell below the bottom layer.
  "ADBC" has caps 1 cell tall and starts at the mark's left edge; the URL has
  an x-height of half a cell and ends at its right edge.

.. card::

   .. image:: _static/visual_identity/adbc-hex-badge-isometric-construction-grid.svg
      :alt: Construction grid for the hex badge
      :width: 100%

Usage
=====

- Use the dimetric logomark everywhere except the hex badge. The isometric
  version is approved for the badge only.
- Don't put the dimetric logomark in a regular hexagon. Its 37° edges can't
  run parallel to the hexagon's 30° sides, so the gaps between them taper.
- For stickers, use the supplied die-cut file. A sticker maker's automatic
  outline decides the cut's angles and joins for itself; the approved cut runs
  parallel to the chevrons and places each join deliberately.

.. grid:: 1 2 2 2
   :gutter: 3

   .. grid-item-card:: Do

      .. image:: _static/visual_identity/adbc-hex-badge-do.png
         :alt: The approved hex badge, with notes showing that the gaps
               between the logo and the hexagon stay even
         :width: 100%
         :align: center

   .. grid-item-card:: Don't

      .. image:: _static/visual_identity/adbc-hex-badge-dont.png
         :alt: The dimetric logomark inside a regular hexagon, crossed out,
               with notes showing that the gaps between them taper
         :width: 100%
         :align: center

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
     - `SVG <_static/visual_identity/adbc-logomark-dimetric-construction-grid.svg>`__,
       `PNG <_static/visual_identity/adbc-logomark-dimetric-construction-grid.png>`__
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
