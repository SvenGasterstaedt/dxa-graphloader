% @file		abschlussarbeit.cls
% 
% @brief	Dokumentklasse fuer Abschlussarbeiten (BA, PA, MA)
%
% @author	Stefan Nothaas, Arbeitsgruppe Betriebssysteme, Institut fuer Informatik, HHU Düsseldorf
%
% @since	20. November 2015
%

\NeedsTeXFormat{LaTeX2e}
\ProvidesClass{abschlussarbeit}[2015/11/18 Vorlage fuer Abschlussarbeiten]

\LoadClass[11pt,twoside,a4paper]{book}

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% packages
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% base packages
\usepackage[utf8]{inputenc}
\usepackage{cite}
\usepackage[german, english]{babel} 
\usepackage{pslatex}
\usepackage{csvsimple}
\usepackage[inner=4cm,outer=3cm,top=3cm,bottom=4cm,includeheadfoot]{geometry}
\usepackage{fancyhdr}
\usepackage{graphicx}

% captions and figures
\usepackage{subcaption}

% for enumerations
\usepackage{enumitem}

% for floats (tables, images)
\usepackage{float}

% for multirow tables
\usepackage{multirow}

% for todo notes
\usepackage{todonotes}

% for setting pdf document properties
% and clickable links in table of contents and on references
\usepackage{color}
\usepackage{hyperref}

% for pseudocode
\usepackage[]{algorithm2e}

% for conditional compilation if-then blocks
\usepackage{ifthen}

% to add chapters like listoffigures to table of contents without page number
\usepackage{tocloft}

% for embedding actual code with syntax highlighting
\usepackage{listings}
\usepackage{textcomp}

% to create draft environment
\usepackage{fancyvrb}

% for underlining
\usepackage{ulem}

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% draft environment for quick notes without proper latex formating
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

\DefineVerbatimEnvironment{draft}
 {Verbatim}
 {fontsize=\footnotesize,formatcom=\color{blue}}

% Uncomment to remove the notes from PDF. Use the comment package
%\excludecomment{todos} 

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% setup listings (make it prettier and have some more color)
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

\definecolor{lbcolor}{rgb}{1.0,1.0,1.0}
\lstset{
	backgroundcolor=\color{lbcolor},
	tabsize=4,
	rulecolor=,
	language=c,
        basicstyle=\scriptsize,
        upquote=true,
        aboveskip={1.5\baselineskip},
        columns=fixed,
        showstringspaces=false,
        extendedchars=true,
        breaklines=true,
        prebreak = \raisebox{0ex}[0ex][0ex]{\ensuremath{\hookleftarrow}},
        frame=single,
        showtabs=false,
        showspaces=false,
        showstringspaces=false,
        identifierstyle=\ttfamily,
        keywordstyle=\color[rgb]{0,0,1},
        commentstyle=\color[rgb]{0.133,0.545,0.133},
        stringstyle=\color[rgb]{0.627,0.126,0.941},
	numbers=left,
	stepnumber=1,    
	firstnumber=1,
	numberfirstline=true,
}

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% setup algorithm environment
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% german description
\SetAlgorithmName{Algorithmus}{algorithmus}{Algorithmenverzeichnis}

\newtheorem{mydef}{Definition}

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% clickable table of contents, links, chapters and cites
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

\hypersetup{
    colorlinks=true,
    citecolor=black,
    filecolor=black,
    linkcolor=black,
    urlcolor=black,
    linkbordercolor=red,
    citebordercolor=green,
    urlbordercolor=cyan,
}

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% document variables
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

\newcommand{\AutorVorname}[1]{\newcommand{\autorvorname}{#1}}
\newcommand{\AutorNachname}[1]{\newcommand{\autornachname}{#1}}
\newcommand{\AutorGeburtsort}[1]{\newcommand{\autorgeburtsort}{#1}}
\newcommand{\AbschlussarbeitTitel}[1]{\newcommand{\abschlussarbeittitel}{#1}}
\newcommand{\AbschlussarbeitTyp}[1]{\newcommand{\abschlussarbeittyp}{#1}}
\newcommand{\AbschlussarbeitKeywords}[1]{\newcommand{\abschlussarbeitkeywords}{#1}}
\newcommand{\Ort}[1]{\newcommand{\ort}{#1}}
\newcommand{\Datum}[1]{\newcommand{\datum}{#1}}
\newcommand{\Erstgutachter}[1]{\newcommand{\erstgutachter}{#1}}
\newcommand{\Zweitgutachter}[1]{\newcommand{\zweitgutachter}{#1}}
\newcommand{\Betreuer}[1]{\newcommand{\betreuer}{#1}}
\newcommand{\LiteraturBibtexDatei}[1]{\newcommand{\literaturbibtexdatei}{#1}}
\newcommand{\AnhangDatei}[1]{\newcommand{\anhangdatei}{#1}}

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% options/switches
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

\newboolean{opt_titelblatt}
\setboolean{opt_titelblatt}{false}
\newcommand{\Titelblatt}{\setboolean{opt_titelblatt}{true}}

\newboolean{opt_todoliste}
\setboolean{opt_todoliste}{false}
\newcommand{\TodoListe}{\setboolean{opt_todoliste}{true}}

\newboolean{opt_inhaltsverzeichnis}
\setboolean{opt_inhaltsverzeichnis}{false}
\newcommand{\Inhaltsverzeichnis}{\setboolean{opt_inhaltsverzeichnis}{true}}

\newboolean{opt_abbildungsverzeichnis}
\setboolean{opt_abbildungsverzeichnis}{false}
\newcommand{\Abbildungsverzeichnis}{\setboolean{opt_abbildungsverzeichnis}{true}}

\newboolean{opt_tabellenverzeichnis}
\setboolean{opt_tabellenverzeichnis}{false}
\newcommand{\Tabellenverzeichnis}{\setboolean{opt_tabellenverzeichnis}{true}}

\newboolean{opt_algorithmenverzeichnis}
\setboolean{opt_algorithmenverzeichnis}{false}
\newcommand{\Algorithmenverzeichnis}{\setboolean{opt_algorithmenverzeichnis}{true}}

\newboolean{opt_anhang}
\setboolean{opt_anhang}{false}
\newcommand{\Anhang}{\setboolean{opt_anhang}{true}}

\newboolean{opt_erklaerung}
\setboolean{opt_erklaerung}{false}
\newcommand{\Erklaerung}{\setboolean{opt_erklaerung}{true}}

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% set pdf properties
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% make sure this part is placed AFTER the variables are set, but
% still before the actual document section
\AtBeginDocument{
\hypersetup{
  pdfauthor={\autorvorname{} \autornachname},
  pdftitle={\abschlussarbeittitel},
  pdfkeywords={\abschlussarbeitkeywords},
  pdfproducer={LaTeX with hyperref},
  pdfcreator={PDFLaTeX}
}
}

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% abschlussarbeit environment
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

\newenvironment{abschlussarbeit}
{
  \ifthenelse{\boolean{opt_todoliste}}
  {
    \listoftodos[TODOs (Vor dem Druck entfernen!)]
    \newpage
  }{}

  \ifthenelse{\boolean{opt_titelblatt}}
  {
    \setlength{\parindent}{0pt}
    \pagenumbering{roman}
    \titelblatt
    \fancypagestyle{plain}\
    {%
      \fancyhf{} % clear all header and footer fields
      \renewcommand{\headrulewidth}{0pt}
      % \fancyhead[RO,LE]{\scshape\leftmark}
      \fancyfoot[RO,LE]{\thepage}
    }

    \pagestyle{fancy}
    \fancyhf{}
    % \fancyhead[RO,LE]{\scshape\leftmark}
    \fancyfoot[RO,LE]{\thepage}

    \renewcommand{\headrulewidth}{0pt}

    \renewcommand{\baselinestretch}{1}\normalsize
  }{}

  %%%%%%%%%
  
  \ifthenelse{\boolean{opt_inhaltsverzeichnis}}
  {
    \tableofcontents
  }{}

  \clearpage{\pagestyle{empty}\cleardoublepage}
  %\renewcommand{\thepage}{\arabic{page}}
  %\setcounter{page}{1}
  
  %%%%%%%%%
  
  \pagenumbering{arabic}
  %\fancyhead[RO,LE]{\scshape\leftmark}
  \renewcommand{\headrulewidth}{0.4pt}
}
{
  \appendix
  \include{\anhangdatei}
  
  \cleardoublepage
  
  \literatur
  \cleardoublepage
  
  \ifthenelse{\boolean{opt_abbildungsverzeichnis}}
  {
    \addcontentsline{toc}{chapter}{Abbildungsverzeichnis}
    \listoffigures
    \cleardoublepage
  }{}
  \ifthenelse{\boolean{opt_tabellenverzeichnis}}
  {
    \addcontentsline{toc}{chapter}{Tabellenverzeichnis}
    \listoftables
    \cleardoublepage
  }{}
  \ifthenelse{\boolean{opt_algorithmenverzeichnis}}
  {
    \addcontentsline{toc}{chapter}{Algorithmenverzeichnis}
    \listofalgorithms
  }{}
  
  \erklaerung
}

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% titelblatt
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

\newcommand{\titelblatt}
{
  %\renewcommand{\baselinestretch}{0.5}\normalsize
  \begin{titlepage}
    \begin{figure}[htb]
      \begin{center}
	\includegraphics[width=0.4\textwidth]{template/hhu_logo.jpg}
      \end{center}
    \end{figure}
    \begin{center}
      \renewcommand{\baselinestretch}{0.5}\normalsize
      %\vspace{2cm}
      \huge
      \textbf
      {
	\abschlussarbeittitel
	\\
      }
      \renewcommand{\baselinestretch}{1.25}\normalsize
      \large
      \vspace{0.5cm}
      \abschlussarbeittyp\\
      von\\
      %\vspace{0.5cm}
      \huge
      \textbf{\autorvorname{} \autornachname}\\
      \vspace{0.5cm}
      \large
      aus\\
      \autorgeburtsort\\
      \vspace{0.5cm}
      vorgelegt an der\\
      \vspace{0.5cm}
      Abteilung Betriebssysteme\\
      Prof. Dr. Michael Schöttner\\
      Heinrich-Heine-Universität Düsseldorf\\
      \vspace{0.5cm}
      \datum\\
      \vspace{0.5cm}
      Gutachter:\\
      \erstgutachter\\
      Zweitgutachter:\\
      \zweitgutachter\\
      Betreuer:\\
      \betreuer\\
    \end{center}
  \end{titlepage}
  \clearpage{\pagestyle{empty}\cleardoublepage}
}

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% literatur
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

\newcommand{\literatur}
{
  \bibliography{\literaturbibtexdatei}
  \bibliographystyle{plain}

  \addcontentsline{toc}{chapter}{Literaturverzeichnis}
  \cleardoublepage
  %\bibliographystyle{alpha}
  %\bibliography{books}

  \clearpage{\pagestyle{empty}\cleardoublepage}
}

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% ehrenwoertliche erklaerung
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

\newcommand{\erklaerung}
{
  \cleardoublepage
  \pagestyle{empty}
  \huge
  \noindent\textbf{Ehrenwörtliche Erklärung}

  \large
  \vspace{3cm}
  \noindent Hiermit versichere ich, die vorliegende \abschlussarbeittyp $\;$selbstständig
  verfasst und keine anderen als die angegebenen Quellen und Hilfsmittel benutzt
  zu haben. Alle Stellen, die aus den Quellen entnommen wurden, sind als solche
  kenntlich gemacht worden. Diese Arbeit hat in gleicher oder ähnlicher Form noch
  keiner Prüfungsbehörde vorgelegen.

  \vspace{6cm}

  \noindent \autornachname, \autorvorname
  
  \vspace{1.5cm}


  \noindent \ort, \datum

  \clearpage{\pagestyle{empty}\cleardoublepage}
}

\newcommand{\abstractname}
{
\centerline{\uline{\bfseries word}}
}
