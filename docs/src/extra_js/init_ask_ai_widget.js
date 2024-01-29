document.addEventListener("DOMContentLoaded", function () {
    var script = document.createElement("script");
    script.src = "https://widget.kapa.ai/kapa-widget.bundle.js";
    script.setAttribute("data-website-id", "c5881fae-cec0-490b-b45e-d83d131d4f25");
    script.setAttribute("data-project-name", "LanceDB");
    script.setAttribute("data-project-color", "#000000");
    script.setAttribute("data-project-logo", "https://avatars.githubusercontent.com/u/108903835?s=200&v=4");
  script.setAttribute("data-modal-example-questions","Help me create an IVF_PQ index,How do I do an exhaustive search?,How do I create a LanceDB table?,Can I use my own embedding function?");
    script.async = true;
    document.head.appendChild(script);
  });