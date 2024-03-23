// Creates an SVG robot icon (from Lucide)
function robotSVG() {
  var svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
  svg.setAttribute("width", "24");
  svg.setAttribute("height", "24");
  svg.setAttribute("viewBox", "0 0 24 24");
  svg.setAttribute("fill", "none");
  svg.setAttribute("stroke", "currentColor");
  svg.setAttribute("stroke-width", "2");
  svg.setAttribute("stroke-linecap", "round");
  svg.setAttribute("stroke-linejoin", "round");
  svg.setAttribute("class", "lucide lucide-bot-message-square");

  var path1 = document.createElementNS("http://www.w3.org/2000/svg", "path");
  path1.setAttribute("d", "M12 6V2H8");
  svg.appendChild(path1);

  var path2 = document.createElementNS("http://www.w3.org/2000/svg", "path");
  path2.setAttribute("d", "m8 18-4 4V8a2 2 0 0 1 2-2h12a2 2 0 0 1 2 2v8a2 2 0 0 1-2 2Z");
  svg.appendChild(path2);

  var path3 = document.createElementNS("http://www.w3.org/2000/svg", "path");
  path3.setAttribute("d", "M2 12h2");
  svg.appendChild(path3);

  var path4 = document.createElementNS("http://www.w3.org/2000/svg", "path");
  path4.setAttribute("d", "M9 11v2");
  svg.appendChild(path4);

  var path5 = document.createElementNS("http://www.w3.org/2000/svg", "path");
  path5.setAttribute("d", "M15 11v2");
  svg.appendChild(path5);

  var path6 = document.createElementNS("http://www.w3.org/2000/svg", "path");
  path6.setAttribute("d", "M20 12h2");
  svg.appendChild(path6);

  return svg
}

// Creates the Fluidic Chatbot buttom
function fluidicButton() {
  var btn = document.createElement("a");
  btn.href = "https://asklancedb.com";
  btn.target = "_blank";
  btn.style.position = "fixed";
  btn.style.fontWeight = "bold";
  btn.style.fontSize = ".8rem";
  btn.style.right = "10px";
  btn.style.bottom = "10px";
  btn.style.width = "80px";
  btn.style.height = "80px";
  btn.style.background = "linear-gradient(135deg, #7C5EFF 0%, #625eff 100%)";
  btn.style.color = "white";
  btn.style.borderRadius = "5px";
  btn.style.display = "flex";
  btn.style.flexDirection = "column";
  btn.style.justifyContent = "center";
  btn.style.alignItems = "center";
  btn.style.zIndex = "1000";
  btn.style.opacity = "0";
  btn.style.boxShadow = "0 0 0 rgba(0, 0, 0, 0)";
  btn.style.transition = "opacity 0.2s ease-in, box-shadow 0.2s ease-in";

  setTimeout(function() {
      btn.style.opacity = "1";
      btn.style.boxShadow = "0 0 .2rem #0000001a,0 .2rem .4rem #0003"
  }, 0);

  return btn
}

document.addEventListener("DOMContentLoaded", function() {
  var btn = fluidicButton()
  btn.appendChild(robotSVG());
  var text = document.createTextNode("Ask AI");
  btn.appendChild(text);
  document.body.appendChild(btn);
});
