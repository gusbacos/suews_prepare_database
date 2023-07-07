1. Testade att göra tz-lagret mindre. Körde v.generalize och fick ner det till typ 1 mB när jag tagit bort kolumner från db också
2. Behövs region längre eftersom vi använder oss av LUCY-länder? Om inte kan vi flytta över till Processing toolbox istället.
3. GridLayoutXX.nml-info behövs för varje typologi. Istället för ESTM typ... Om jag fattat det rätt. 
4. Måste kolla med REading hur vi gör med building_scale på höga höjder där vi får orealistiska höga tal
5. Göra check på att id i grid är integer
6. Fixa fel om det inte finns några byggnader och/eller vegetation i write_gridlayuout