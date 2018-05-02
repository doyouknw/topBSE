var dataTable = document.querySelector('.dataTable');
var tableData;
var tableHeaders = [...document.querySelectorAll(".dataTable>thead>tr>th")];
var tbody = dataTable.querySelector('tbody');
var rows = tbody.querySelectorAll('tr');
var visibleRows = rows.length;
var dataFields = tableHeaders.map(header => {
        return header.dataset.field.toLowerCase();
    });
var search;
var start = null, end = null, size = null;
$(document).on('change', '#search', function(e){
    search = $(this).val();
    if(!search){
        location.reload();
    }
    $.ajax({
       url: '/searchResponse',
       method: 'GET',
       data: {'name' : search },
       success: function(a){
           if(a.length != 0){
               tableData = a;
               size = tableData.length;
               loadTable();
           }
           return false;
       },
       error: function(err){
           console.error(err);
       }
    });
});

function rowHtml(data){
    var html = dataFields.reduce((html, field) => {
        var regex = new RegExp(search, "i");
        if(field == "name"){
            return html + "<td>" + data[field].replace(regex,
                        `<span style="background: #ffc600; color: #000;">${search.toUpperCase()}</span>`) + "</td>"
        }
        else
            return html + "<td>" + data[field] + "</td>";
    }, "");
    return html;
}

function loadTable(){
    for(var i = 0; i < visibleRows; i++){
        var indx = start + i;
        rows[i].innerHTML = rowHtml(tableData[indx]);
    }
    if(size > visibleRows){
        $('#msg').html('<p class="help-block">Scroll UP/DOWN to load more!</p>');
    }
}
$('tbody').bind('mousewheel DOMMouseScroll', function(e){
    if(e.originalEvent.wheelDelta > 0 || e.originalEvent.detail < 0){
       // load top
        loadMore("up");
    }
    else{
        // load down
        loadMore("down");
    }
});
function loadMore(dirc, rowsPerScroll = 1) {
    if (search !== null && (start > 0 || end < size)) {
        if (dirc === "up") {
            start = Math.max(0, start - rowsPerScroll);
            end = start + visibleRows;
        } else if (dirc === "down") {
            end = Math.min(size, end + rowsPerScroll);
            start = end - visibleRows;
        }
        loadTable();
    }
}